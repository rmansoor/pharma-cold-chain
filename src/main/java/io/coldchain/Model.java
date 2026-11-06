package io.coldchain;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public final class Model {
 public static final StructType EVENT=new StructType().add("eventId","string").add("revision","long")
  .add("kind","string").add("eventTime","timestamp").add("productId","string").add("productCode","string")
  .add("lotId","string").add("containerId","string").add("sensorId","string")
  .add("startTime","timestamp").add("endTime","timestamp").add("temperatureC","double")
  .add("minC","double").add("maxC","double").add("maxExcursionSeconds","long")
  .add("location","string").add("status","string");
 private Model() {}
 static final String SKEW="kafkaTime + INTERVAL 5 MINUTES";
 static final String KINDS="kind IN ('READING','PLACEMENT','ASSIGNMENT','SPEC','INVENTORY')";
 static final String INTERVAL_KINDS="kind IN ('READING','PLACEMENT','ASSIGNMENT','SPEC')";
 static final String TOPIC="CASE kind WHEN 'READING' THEN 'coldchain.sensors' WHEN 'PLACEMENT' THEN 'coldchain.traceability' "
  +"WHEN 'ASSIGNMENT' THEN 'coldchain.traceability' WHEN 'SPEC' THEN 'coldchain.specifications' WHEN 'INVENTORY' THEN 'coldchain.inventory' END";
 // A rule applies when its condition holds and yields its reason code when the check is false or unknown.
 static String rule(String code,String when,String ok){return "IF(coalesce("+when+",false) AND NOT coalesce("+ok+",false),'"+code+"',NULL)";}
 // Identifiers need a non-whitespace character; trim() alone would let tabs and newlines through.
 static String id(String field,String when){return rule("INVALID_ID:"+field,when,field+" RLIKE '\\\\S'");}
 // BETWEEN also rejects NaN and infinities.
 static String celsius(String field){return field+" BETWEEN -100 AND 100";}
 // Reason codes in a fixed order (docs/event-contracts.md). Intervals are half-open [startTime,endTime) in UTC at
 // whole-second precision, because durations are counted in seconds.
 static final List<String> RULES=List.of(id("eventId","true"),rule("INVALID_REVISION","true","revision >= 1"),
  rule("INVALID_KIND","true",KINDS),rule("WRONG_TOPIC",TOPIC+" IS NOT NULL","topic = "+TOPIC),
  rule("MISSING_EVENT_TIME","true","eventTime IS NOT NULL"),rule("EVENT_TIME_AHEAD_OF_KAFKA","true","eventTime IS NULL OR eventTime <= "+SKEW),
  id("sensorId","kind IN ('READING','ASSIGNMENT')"),id("productId","kind IN ('PLACEMENT','INVENTORY')"),
  id("productCode","kind IN ('PLACEMENT','SPEC')"),id("lotId","kind = 'PLACEMENT'"),
  id("containerId","kind IN ('PLACEMENT','ASSIGNMENT')"),id("location","kind = 'INVENTORY'"),
  rule("INVALID_INTERVAL",INTERVAL_KINDS,"startTime < endTime"),
  rule("FRACTIONAL_SECONDS",INTERVAL_KINDS,"coalesce(startTime = date_trunc('SECOND',startTime),true) AND coalesce(endTime = date_trunc('SECOND',endTime),true)"),
  rule("READING_ENDS_AHEAD_OF_KAFKA","kind = 'READING'","endTime IS NULL OR endTime <= "+SKEW),
  rule("OUT_OF_RANGE:temperatureC","kind = 'READING'",celsius("temperatureC")),
  rule("OUT_OF_RANGE:minC","kind = 'SPEC'",celsius("minC")),rule("OUT_OF_RANGE:maxC","kind = 'SPEC'",celsius("maxC")),
  rule("SPEC_MIN_ABOVE_MAX","kind = 'SPEC'","isnan(minC) OR isnan(maxC) OR coalesce(minC <= maxC,true)"),
  rule("INVALID_EXCURSION_ALLOWANCE","kind = 'SPEC'","maxExcursionSeconds >= 0"),
  rule("INVALID_STATUS","kind = 'INVENTORY'","status IN ('IN_STOCK','DISPATCHED')"));
 public static Dataset<Row> parse(Dataset<Row> bronze) {
  Column[] fields=Arrays.stream(EVENT.fieldNames()).map(functions::col).toArray(Column[]::new);
  return bronze.withColumn("e",from_json(col("raw"),EVENT)).selectExpr("*","e.*").drop("e")
   // Hash parsed values, not bytes: key order, whitespace and timestamp spelling do not change meaning.
   .withColumn("payloadHash",sha2(to_json(struct(fields),Map.of("timestampFormat","yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")),256))
   // Text that is not a strict JSON object (RFC 8259: no single quotes, NaN or Infinity) gets one code; field
   // rules would only repeat it. Spark's JSON reader accepts those by default.
   .withColumn("reasons",expr("IF(from_json(raw,'map<string,string>',map('allowSingleQuotes','false','allowNonNumericNumbers','false')) IS NULL,array('MALFORMED_JSON'),filter(array("+String.join(",",RULES)+"),r -> r IS NOT NULL))"))
   .withColumn("valid",expr("size(reasons) = 0"));
 }
 static Column hash(String... fields){return sha2(to_json(struct(Arrays.stream(fields).map(functions::col).toArray(Column[]::new))),256);}
 // Invalid records are kept; one is resolved once its eventId has a valid revision above the invalid one.
 public static Dataset<Row> quarantine(Dataset<Row> parsed) {
  Dataset<Row> latest=parsed.filter("valid").groupBy("eventId").agg(max("revision").alias("latestValid"));
  return parsed.filter("NOT valid").join(latest,"eventId","left").withColumn("caseId",hash("topic","partition","offset"))
   .withColumn("superseded",expr("coalesce(latestValid > coalesce(revision,0),false)"))
   .selectExpr("caseId","topic","partition","offset","kafkaTime","sourceSystem","ingestedAt","raw","eventId","revision","reasons",
    "IF(superseded,'RESOLVED','OPEN') status","IF(superseded,'SUPERSEDED_BY_VALID_REVISION',NULL) resolution",
    "IF(superseded,latestValid,NULL) resolvedByRevision");
 }
 // One ID and revision must mean the same thing on every retry, and an ID keeps its kind across revisions.
 // Every competing variant is listed with its source coordinates; no variant is chosen.
 public static Dataset<Row> conflictCases(Dataset<Row> valid) {
  Column variants=sort_array(collect_list(struct(col("payloadHash"),col("kind"),col("revision"),col("topic"),col("partition"),
   col("offset"),col("sourceSystem"),col("kafkaTime")))).alias("variants");
  Dataset<Row> payloads=valid.groupBy("eventId","revision").agg(countDistinct("payloadHash").alias("n"),variants)
   .filter("n > 1").withColumn("caseType",lit("PAYLOAD_MISMATCH"));
  Dataset<Row> kinds=valid.groupBy("eventId").agg(countDistinct("kind").alias("n"),variants)
   .filter("n > 1").withColumn("caseType",lit("KIND_MISMATCH")).withColumn("revision",lit(null).cast("long"));
  Dataset<Row> latest=valid.groupBy("eventId").agg(max("revision").alias("latest"));
  // A higher revision from the source supersedes a disputed one. A kind mismatch has no automatic resolution.
  return payloads.unionByName(kinds).join(latest,"eventId").withColumn("caseId",hash("caseType","eventId","revision"))
   .withColumn("superseded",expr("caseType = 'PAYLOAD_MISMATCH' AND latest > revision"))
   .selectExpr("caseId","caseType","eventId","revision","variants","IF(superseded,'RESOLVED','OPEN') status",
    "IF(superseded,'SUPERSEDED_BY_HIGHER_REVISION',NULL) resolution","IF(superseded,latest,NULL) resolvedByRevision");
 }
 // IDs withheld from Silver: any open conflict case blocks every revision of the ID.
 public static Dataset<Row> conflicts(Dataset<Row> valid) {
  return conflictCases(valid).filter("status = 'OPEN'").select("eventId").distinct();
 }
 // Current state of every case, for the append-only audit log.
 public static Dataset<Row> caseStates(Dataset<Row> quarantine,Dataset<Row> cases) {
  String[] cols={"caseId","caseType","eventId","revision","status","resolution","resolvedByRevision"};
  return quarantine.withColumn("caseType",lit("INVALID_CONTRACT")).selectExpr(cols).unionByName(cases.selectExpr(cols));
 }
 public static Dataset<Row> canonical(Dataset<Row> valid) {
  // The highest revision wins regardless of arrival order; gaps between revisions are allowed.
  return valid.join(conflicts(valid),"eventId","left_anti")
   .withColumn("rn",row_number().over(Window.partitionBy("eventId")
    .orderBy(col("revision").desc(),col("kafkaTime").desc(),col("partition").desc(),col("offset").desc())))
   .filter("rn=1").drop("rn","valid","reasons");
 }
 public static Dataset<Row> assessments(SparkSession spark, Dataset<Row> events) {
  events.createOrReplaceTempView("events");
  spark.sql("SELECT * FROM events WHERE kind='PLACEMENT'").createOrReplaceTempView("placements");
  // Split intervals at every placement, sensor assignment, reading and specification boundary.
  spark.sql("""
   SELECT p.productId, p.eventId placementId, p.lotId, p.productCode, r.eventId readingId,
    a.eventId assignmentId, s.eventId specificationId,
    greatest(p.startTime,a.startTime,r.startTime,s.startTime) segmentStart,
    least(p.endTime,a.endTime,r.endTime,s.endTime) segmentEnd,
    r.temperatureC, s.minC, s.maxC, s.maxExcursionSeconds
   FROM placements p
   JOIN events a ON a.kind='ASSIGNMENT' AND a.containerId=p.containerId
   JOIN events r ON r.kind='READING' AND r.sensorId=a.sensorId
   JOIN events s ON s.kind='SPEC' AND s.productCode=p.productCode
   WHERE greatest(p.startTime,a.startTime,r.startTime,s.startTime)
       < least(p.endTime,a.endTime,r.endTime,s.endTime)
   """).createOrReplaceTempView("segments");
  // Union intervals before summing: overlapping readings must not inflate exposure or coverage.
  spark.sql("""
   SELECT productId, 'COVERAGE' metric, segmentStart, segmentEnd FROM segments
   UNION ALL
   SELECT productId, 'UNSAFE', segmentStart, segmentEnd FROM segments
   WHERE temperatureC < minC OR temperatureC > maxC
   UNION ALL
   SELECT productId, 'EXPECTED', startTime, endTime FROM placements
   """).createOrReplaceTempView("intervals");
  spark.sql("""
   WITH previous AS (
    SELECT *, max(segmentEnd) OVER (PARTITION BY productId,metric ORDER BY segmentStart,segmentEnd
     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) previousEnd FROM intervals
   ), flags AS (
    SELECT *, IF(previousEnd IS NULL OR segmentStart > previousEnd,1,0) newGroup FROM previous
   ), numbered AS (
    SELECT *, sum(newGroup) OVER (PARTITION BY productId,metric ORDER BY segmentStart,segmentEnd
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) island FROM flags
   ), merged AS (
    SELECT productId,metric,island,min(segmentStart) a,max(segmentEnd) b
    FROM numbered GROUP BY productId,metric,island
   )
   SELECT productId,metric,sum(unix_timestamp(b)-unix_timestamp(a)) seconds
   FROM merged GROUP BY productId,metric
   """).createOrReplaceTempView("durations");
  spark.sql("""
   SELECT productId, sum(IF(metric='EXPECTED',seconds,0)) expectedSeconds,
    sum(IF(metric='COVERAGE',seconds,0)) coveredSeconds,
    sum(IF(metric='UNSAFE',seconds,0)) excursionSeconds FROM durations GROUP BY productId
   """).createOrReplaceTempView("metrics");
  spark.sql("""
   SELECT productId, sort_array(collect_set(readingId)) readingIds,
    sort_array(collect_set(assignmentId)) assignmentIds,
    sort_array(collect_set(specificationId)) specificationIds,
    min(maxExcursionSeconds) allowedExcursionSeconds
   FROM segments GROUP BY productId
   """).createOrReplaceTempView("evidence");
  spark.sql("""
   SELECT productId, location, status FROM (
    SELECT *,row_number() OVER(PARTITION BY productId ORDER BY eventTime DESC,revision DESC,eventId DESC) n
    FROM events WHERE kind='INVENTORY'
   ) WHERE n=1
   """).createOrReplaceTempView("inventory");
  return spark.sql("""
   SELECT m.*, coalesce(e.allowedExcursionSeconds,0L) allowedExcursionSeconds,
    e.readingIds,e.assignmentIds,e.specificationIds,
    coalesce(i.location,'UNKNOWN') location,coalesce(i.status,'UNKNOWN') inventoryStatus,
    m.expectedSeconds-m.coveredSeconds missingSeconds,
    CASE WHEN m.excursionSeconds > e.allowedExcursionSeconds THEN 'QUALITY_REVIEW'
     WHEN m.coveredSeconds < m.expectedSeconds OR e.allowedExcursionSeconds IS NULL THEN 'INSUFFICIENT_DATA'
     ELSE 'NO_OBSERVED_EXCURSION' END assessment,
    'exposure-v1' ruleVersion
   FROM metrics m LEFT JOIN evidence e USING(productId) LEFT JOIN inventory i USING(productId)
   """).withColumn("assessmentHash",sha2(to_json(struct(col("expectedSeconds"),col("coveredSeconds"),
    col("excursionSeconds"),col("allowedExcursionSeconds"),col("assessment"),col("location"),
    col("inventoryStatus"),col("readingIds"),col("assignmentIds"),col("specificationIds"),col("ruleVersion"))),256));
 }
}

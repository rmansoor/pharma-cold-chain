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
 // Identifiers need a non-whitespace character; trim() alone would let tabs and newlines through.
 static String ids(String... names){return String.join(" AND ",Arrays.stream(names).map(n->n+" RLIKE '\\\\S'").toList());}
 // Half-open [startTime,endTime) in UTC at whole-second precision, because durations are counted in seconds.
 static final String INTERVAL="startTime < endTime AND startTime = date_trunc('SECOND',startTime) AND endTime = date_trunc('SECOND',endTime)";
 // BETWEEN also rejects NaN and infinities.
 static final String CELSIUS=" BETWEEN -100 AND 100";
 static final String SKEW="kafkaTime + INTERVAL 5 MINUTES";
 public static Dataset<Row> parse(Dataset<Row> bronze) {
  Column[] fields=Arrays.stream(EVENT.fieldNames()).map(functions::col).toArray(Column[]::new);
  return bronze.withColumn("e",from_json(col("raw"),EVENT)).selectExpr("*","e.*").drop("e")
   // Hash parsed values, not bytes: key order, whitespace and timestamp spelling do not change meaning.
   .withColumn("payloadHash",sha2(to_json(struct(fields),Map.of("timestampFormat","yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")),256))
   .withColumn("valid",coalesce(expr(ids("eventId")+" AND revision >= 1 "
    +"AND eventTime IS NOT NULL AND eventTime <= "+SKEW+" AND ("
    +"(kind = 'READING' AND "+ids("sensorId")+" AND "+INTERVAL+" AND endTime <= "+SKEW+" AND temperatureC"+CELSIUS
    +" AND topic = 'coldchain.sensors') OR "
    +"(kind = 'PLACEMENT' AND "+ids("productId","productCode","lotId","containerId")+" AND "+INTERVAL
    +" AND topic = 'coldchain.traceability') OR "
    +"(kind = 'ASSIGNMENT' AND "+ids("sensorId","containerId")+" AND "+INTERVAL
    +" AND topic = 'coldchain.traceability') OR "
    +"(kind = 'SPEC' AND "+ids("productCode")+" AND "+INTERVAL+" AND minC"+CELSIUS+" AND maxC"+CELSIUS+" AND minC <= maxC "
    +"AND maxExcursionSeconds >= 0 AND topic = 'coldchain.specifications') OR "
    +"(kind = 'INVENTORY' AND "+ids("productId","location")+" AND status IN ('IN_STOCK','DISPATCHED') "
    +"AND topic = 'coldchain.inventory'))"),lit(false)));
 }
 public static Dataset<Row> conflicts(Dataset<Row> valid) {
  // One ID and revision must mean the same thing on every retry, and an ID keeps its kind across revisions.
  Dataset<Row> payloads=valid.groupBy("eventId","revision").agg(countDistinct("payloadHash").alias("variants"))
   .filter("variants > 1").select("eventId");
  Dataset<Row> kinds=valid.groupBy("eventId").agg(countDistinct("kind").alias("kinds")).filter("kinds > 1").select("eventId");
  return payloads.union(kinds).distinct();
 }
 public static Dataset<Row> canonical(Dataset<Row> valid) {
  // The highest revision wins regardless of arrival order; gaps between revisions are allowed.
  return valid.join(conflicts(valid),"eventId","left_anti")
   .withColumn("rn",row_number().over(Window.partitionBy("eventId")
    .orderBy(col("revision").desc(),col("kafkaTime").desc(),col("partition").desc(),col("offset").desc())))
   .filter("rn=1").drop("rn","valid");
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

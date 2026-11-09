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
  .add("location","string").add("status","string")
  .add("childId","string").add("childLevel","string").add("parentId","string").add("parentLevel","string")
  .add("custodianId","string").add("temperature","double").add("temperatureUnit","string").add("uncertaintyC","double")
  .add("synthetic","boolean");
 private Model() {}
 static final String SKEW="kafkaTime + INTERVAL 5 MINUTES";
 static final String KINDS="kind IN ('READING','PLACEMENT','ASSIGNMENT','SPEC','INVENTORY','PACKING','CUSTODY','CALIBRATION')";
 static final String INTERVAL_KINDS="kind IN ('READING','PLACEMENT','ASSIGNMENT','SPEC','PACKING','CUSTODY','CALIBRATION')";
 static final String UNIT_PACKING="(kind = 'PACKING' AND childLevel = 'UNIT')";
 static String rank(String level){return "array_position(array('UNIT','CARTON','PALLET','CONTAINER'),"+level+")";}
 static final String TOPIC="CASE kind WHEN 'READING' THEN 'coldchain.sensors' WHEN 'CALIBRATION' THEN 'coldchain.sensors' WHEN 'PLACEMENT' THEN 'coldchain.traceability' "
  +"WHEN 'ASSIGNMENT' THEN 'coldchain.traceability' WHEN 'PACKING' THEN 'coldchain.traceability' WHEN 'CUSTODY' THEN 'coldchain.traceability' WHEN 'SPEC' THEN 'coldchain.specifications' WHEN 'INVENTORY' THEN 'coldchain.inventory' END";
 // A rule applies when its condition holds and yields its reason code when the check is false or unknown.
 static String rule(String code,String when,String ok){return "IF(coalesce("+when+",false) AND NOT coalesce("+ok+",false),'"+code+"',NULL)";}
 // Identifiers need a non-whitespace character; trim() alone would let tabs and newlines through.
 static String id(String field,String when){return rule("INVALID_ID:"+field,when,field+" RLIKE '\\\\S'");}
 // BETWEEN also rejects NaN and infinities.
 static String celsius(String field){return field+" BETWEEN -100 AND 100";}
 // A reading gives either temperatureC or temperature with temperatureUnit; both are normalized to °C, rounded to 1e-6.
 static final String UNITS="('C','F','K')";
 static final String CELSIUS="coalesce(temperatureC,round(CASE temperatureUnit WHEN 'C' THEN temperature WHEN 'F' THEN (temperature - 32) * 5 / 9 "
  +"WHEN 'K' THEN temperature - 273.15 END,6))";
 // Reason codes in a fixed order (docs/event-contracts.md). Intervals are half-open [startTime,endTime) in UTC at
 // whole-second precision, because durations are counted in seconds.
 static final List<String> RULES=List.of(id("eventId","true"),rule("INVALID_REVISION","true","revision >= 1"),
  rule("INVALID_KIND","true",KINDS),rule("WRONG_TOPIC",TOPIC+" IS NOT NULL","topic = "+TOPIC),
  rule("MISSING_EVENT_TIME","true","eventTime IS NOT NULL"),rule("EVENT_TIME_AHEAD_OF_KAFKA","true","eventTime IS NULL OR eventTime <= "+SKEW),
  id("sensorId","kind IN ('READING','ASSIGNMENT','CALIBRATION')"),id("productId","kind IN ('PLACEMENT','INVENTORY')"),
  id("productCode","kind IN ('PLACEMENT','SPEC') OR "+UNIT_PACKING),id("lotId","kind = 'PLACEMENT' OR "+UNIT_PACKING),
  id("containerId","kind IN ('PLACEMENT','ASSIGNMENT','CUSTODY')"),id("location","kind = 'INVENTORY'"),
  id("custodianId","kind = 'CUSTODY'"),id("childId","kind = 'PACKING'"),id("parentId","kind = 'PACKING'"),
  rule("INVALID_LEVEL:childLevel","kind = 'PACKING'","childLevel IN ('UNIT','CARTON','PALLET')"),
  rule("INVALID_LEVEL:parentLevel","kind = 'PACKING'","parentLevel IN ('CARTON','PALLET','CONTAINER')"),
  rule("PARENT_NOT_ABOVE_CHILD","kind = 'PACKING' AND childLevel IN ('UNIT','CARTON','PALLET') AND parentLevel IN ('CARTON','PALLET','CONTAINER')",
   rank("parentLevel")+" > "+rank("childLevel")),
  rule("SELF_CONTAINED","kind = 'PACKING'","childId IS NULL OR parentId IS NULL OR childId <> parentId"),
  // A reading may be a point observation at one instant (startTime = endTime); it is never widened into an interval.
  rule("INVALID_INTERVAL",INTERVAL_KINDS,"startTime < endTime OR (kind = 'READING' AND startTime = endTime)"),
  rule("FRACTIONAL_SECONDS",INTERVAL_KINDS,"coalesce(startTime = date_trunc('SECOND',startTime),true) AND coalesce(endTime = date_trunc('SECOND',endTime),true)"),
  rule("READING_ENDS_AHEAD_OF_KAFKA","kind = 'READING'","endTime IS NULL OR endTime <= "+SKEW),
  rule("AMBIGUOUS_TEMPERATURE","kind = 'READING'","temperatureC IS NULL OR (temperature IS NULL AND temperatureUnit IS NULL)"),
  rule("INVALID_UNIT:temperatureUnit","kind = 'READING' AND temperatureC IS NULL AND (temperature IS NOT NULL OR temperatureUnit IS NOT NULL)",
   "temperatureUnit IN "+UNITS),
  rule("OUT_OF_RANGE:temperatureC","kind = 'READING' AND (temperature IS NULL OR temperatureUnit IN "+UNITS+")",celsius(CELSIUS)),
  rule("OUT_OF_RANGE:uncertaintyC","kind = 'CALIBRATION'","uncertaintyC BETWEEN 0 AND 10"),
  rule("OUT_OF_RANGE:minC","kind = 'SPEC'",celsius("minC")),rule("OUT_OF_RANGE:maxC","kind = 'SPEC'",celsius("maxC")),
  rule("SPEC_MIN_ABOVE_MAX","kind = 'SPEC'","isnan(minC) OR isnan(maxC) OR coalesce(minC <= maxC,true)"),
  rule("INVALID_EXCURSION_ALLOWANCE","kind = 'SPEC'","maxExcursionSeconds >= 0"),
  // A mistyped flag must not silently turn demo thresholds into real ones.
  rule("INVALID_FLAG:synthetic","kind = 'SPEC'","get_json_object(raw,'$.synthetic') IS NULL OR synthetic IS NOT NULL"),
  rule("INVALID_STATUS","kind = 'INVENTORY'","status IN ('IN_STOCK','DISPATCHED')"));
 public static Dataset<Row> parse(Dataset<Row> bronze) {
  Column[] fields=Arrays.stream(EVENT.fieldNames()).map(functions::col).toArray(Column[]::new);
  return bronze.withColumn("e",from_json(col("raw"),EVENT)).selectExpr("*","e.*").drop("e")
   // Hash parsed values, not bytes: key order, whitespace and timestamp spelling do not change meaning.
   .withColumn("payloadHash",sha2(to_json(struct(fields),Map.of("timestampFormat","yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")),256))
   // Text that is not a strict JSON object (RFC 8259: no single quotes, NaN or Infinity) gets one code; field
   // rules would only repeat it. Spark's JSON reader accepts those by default.
   .withColumn("reasons",expr("IF(from_json(raw,'map<string,string>',map('allowSingleQuotes','false','allowNonNumericNumbers','false')) IS NULL,array('MALFORMED_JSON'),filter(array("+String.join(",",RULES)+"),r -> r IS NOT NULL))"))
   .withColumn("valid",expr("size(reasons) = 0")).withColumn("temperatureC",expr(CELSIUS));
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
 // Gold assessments with their evidence: every merged interval behind each duration (gold/exposure_intervals) and every
 // elementary measured segment (silver/exposure_segments), both with exact source revisions.
 // Version of the exposure calculation (intervals and durations); the outcome policy is versioned separately (Policy.VERSION).
 public static final String RULE_VERSION="exposure-v4";
 public record Result(Dataset<Row> assessments,Dataset<Row> intervals,Dataset<Row> segments) {}
 public static Dataset<Row> assessments(SparkSession spark, Dataset<Row> events){return exposure(spark,events).assessments();}
 // Views built from one checkpoint share its column IDs (productId above all), and without adaptive execution Spark cannot
 // always join them apart. Aliasing every output column gives each view its own IDs before the views are joined.
 static void fresh(SparkSession spark,String... views) {
  for(String v:views) {
   Dataset<Row> d=spark.table(v);
   d.select(Arrays.stream(d.columns()).map(c->col(c).as(c)).toArray(Column[]::new)).createOrReplaceTempView(v);
  }
 }
 public static Result exposure(SparkSession spark, Dataset<Row> events) {
  // Materialize once: lineage, timelines and the segment joins below each read events several times, and without this
  // Spark re-plans the whole parse for every reference.
  events=events.localCheckpoint();
  events.createOrReplaceTempView("events");
  // Units are expected wherever a membership puts them; they are covered only through a chain that reaches a container.
  Dataset<Row> edges=Lineage.edges(events),chains=Lineage.paths(edges).localCheckpoint(),
   lineageAnomalies=Lineage.anomalies(edges,chains).localCheckpoint();
  Lineage.unitContainers(edges,lineageAnomalies).localCheckpoint().createOrReplaceTempView("placements");
  edges.filter("childLevel = 'UNIT'").createOrReplaceTempView("units");
  // Assignments and custody that contradict each other at some instant are left out (Timelines.anomalies).
  Dataset<Row> timeline=Timelines.anomalies(events).localCheckpoint();
  Timelines.usable(events,"ASSIGNMENT",timeline).localCheckpoint().createOrReplaceTempView("assignments");
  Timelines.usable(events,"CUSTODY",timeline).createOrReplaceTempView("custody");
  // One specification per product code and instant, after precedence (Specifications.effective).
  Specifications.effective(events).localCheckpoint().createOrReplaceTempView("specs");
  // Split intervals at every resolved container interval, sensor assignment, reading and specification boundary.
  // Point readings (startTime = endTime) never form a segment; they are matched at their instant below. sourceIds names
  // every event a row depends on: the membership chain, the assignment, the reading and the specification.
  spark.sql("""
   SELECT p.productId, p.lineageIds, r.eventId readingId, r.sensorId,
    a.eventId assignmentId, s.specificationId,
    greatest(p.startTime,a.startTime,r.startTime,s.startTime) startTime,
    least(p.endTime,a.endTime,r.endTime,s.endTime) endTime,
    r.temperatureC, s.minC, s.maxC, s.maxExcursionSeconds, s.synthetic,
    r.temperatureC < s.minC OR r.temperatureC > s.maxC outOfRange,
    concat(p.lineageIds,array(a.eventId,r.eventId,s.specificationId)) sourceIds
   FROM placements p
   JOIN assignments a ON a.containerId=p.containerId
   JOIN events r ON r.kind='READING' AND r.sensorId=a.sensorId AND r.startTime < r.endTime
   JOIN specs s ON s.productCode=p.productCode
   WHERE greatest(p.startTime,a.startTime,r.startTime,s.startTime)
       < least(p.endTime,a.endTime,r.endTime,s.endTime)
   """).localCheckpoint().createOrReplaceTempView("segments");
  // The part of each segment measured by a sensor with a valid calibration, with that calibration's uncertainty.
  spark.sql("""
   SELECT g.productId, g.temperatureC, g.minC, g.maxC, c.eventId calibrationId, c.uncertaintyC,
    greatest(g.startTime,c.startTime) startTime, least(g.endTime,c.endTime) endTime, concat(g.sourceIds,array(c.eventId)) sourceIds
   FROM segments g JOIN events c ON c.kind='CALIBRATION' AND c.sensorId=g.sensorId
   WHERE greatest(g.startTime,c.startTime) < least(g.endTime,c.endTime)
   """).createOrReplaceTempView("calibrated");
  spark.sql("""
   SELECT p.productId, p.lineageIds, r.eventId readingId, a.eventId assignmentId, s.specificationId, r.startTime time,
    s.maxExcursionSeconds, s.synthetic, r.temperatureC < s.minC OR r.temperatureC > s.maxC outOfRange,
    concat(p.lineageIds,array(a.eventId,r.eventId,s.specificationId)) sourceIds
   FROM placements p
   JOIN assignments a ON a.containerId=p.containerId
   JOIN events r ON r.kind='READING' AND r.sensorId=a.sensorId AND r.startTime = r.endTime
   JOIN specs s ON s.productCode=p.productCode
   WHERE greatest(p.startTime,a.startTime,s.startTime) <= r.startTime AND r.startTime < least(p.endTime,a.endTime,s.endTime)
   """).createOrReplaceTempView("points");
  spark.sql("""
   SELECT p.productId, c.custodianId, greatest(p.startTime,c.startTime) startTime, least(p.endTime,c.endTime) endTime,
    concat(p.lineageIds,array(c.eventId)) sourceIds
   FROM placements p JOIN custody c ON c.containerId=p.containerId
   WHERE greatest(p.startTime,c.startTime) < least(p.endTime,c.endTime)
   """).createOrReplaceTempView("custodySegments");
  // Raw interval sets, named after the Gold durations they produce. POSSIBLE_EXCURSION is time out of range or, for a
  // calibrated measurement, within its uncertainty of a limit.
  spark.sql("""
   SELECT productId, 'COVERED' metric, startTime, endTime, sourceIds FROM segments
   UNION ALL
   SELECT productId, 'EXCURSION', startTime, endTime, sourceIds FROM segments WHERE outOfRange
   UNION ALL
   SELECT productId, 'CALIBRATED', startTime, endTime, sourceIds FROM calibrated
   UNION ALL
   SELECT productId, 'POSSIBLE_EXCURSION', startTime, endTime, sourceIds FROM segments WHERE outOfRange
   UNION ALL
   SELECT productId, 'POSSIBLE_EXCURSION', startTime, endTime, sourceIds FROM calibrated
   WHERE temperatureC < minC + uncertaintyC OR temperatureC > maxC - uncertaintyC
   UNION ALL
   SELECT childId, 'EXPECTED', startTime, endTime, array(eventId) FROM units
   UNION ALL
   SELECT productId, 'LOCATED', startTime, endTime, lineageIds FROM placements
   UNION ALL
   SELECT p.productId, 'SPECIFIED', greatest(p.startTime,s.startTime), least(p.endTime,s.endTime), concat(p.lineageIds,array(s.specificationId))
   FROM placements p JOIN specs s ON s.productCode=p.productCode
   WHERE greatest(p.startTime,s.startTime) < least(p.endTime,s.endTime)
   UNION ALL
   SELECT productId, 'CUSTODY', startTime, endTime, sourceIds FROM custodySegments
   """).localCheckpoint().createOrReplaceTempView("intervals");
  // Union before summing; differences (MISSING, UNCALIBRATED, UNSPECIFIED, MISSING_CUSTODY) are cut from the merged sets.
  spark.sql(Exposure.islands("intervals","productId,metric")).localCheckpoint().createOrReplaceTempView("merged");
  spark.sql(Exposure.gaps("merged")).createOrReplaceTempView("gapWindows");
  spark.sql("SELECT * FROM merged UNION ALL SELECT * FROM ("+Exposure.islands("gapWindows","productId,metric")+")").createOrReplaceTempView("shapes");
  spark.sql(Exposure.explained("shapes","intervals")+"""
   UNION ALL
   SELECT productId, 'OUT_OF_RANGE_POINT', time, time, sort_array(array_distinct(flatten(collect_list(sourceIds))))
   FROM points WHERE outOfRange GROUP BY productId, time
   """).createOrReplaceTempView("explained");
  spark.sql(Exposure.revisions("explained","productId,metric,startTime,endTime","events")).createOrReplaceTempView("intervalSources");
  Dataset<Row> intervals=spark.sql("""
   SELECT productId, metric, startTime, endTime, unix_timestamp(endTime)-unix_timestamp(startTime) seconds, sources
   FROM intervalSources
   """).localCheckpoint();
  intervals.createOrReplaceTempView("exposureIntervals");
  spark.sql("""
   SELECT productId, sum(IF(metric='EXPECTED',seconds,0)) expectedSeconds,
    sum(IF(metric='COVERED',seconds,0)) coveredSeconds,
    sum(IF(metric='EXCURSION',seconds,0)) excursionSeconds,
    sum(IF(metric='POSSIBLE_EXCURSION',seconds,0)) possibleExcursionSeconds,
    sum(IF(metric='UNCALIBRATED',seconds,0)) uncalibratedSeconds,
    sum(IF(metric='UNSPECIFIED',seconds,0)) unspecifiedSeconds,
    sum(IF(metric='MISSING_CUSTODY',seconds,0)) missingCustodySeconds,
    sum(IF(metric='MISSING',seconds,0)) missingSeconds FROM exposureIntervals GROUP BY productId
   """).createOrReplaceTempView("metrics");
  spark.sql("""
   SELECT productId, sort_array(collect_set(readingId)) readingIds,
    sort_array(collect_set(assignmentId)) assignmentIds,
    sort_array(collect_set(specificationId)) specificationIds,
    sort_array(array_distinct(flatten(collect_list(lineageIds)))) lineageIds,
    min(maxExcursionSeconds) allowedExcursionSeconds, bool_or(synthetic) syntheticThresholds,
    count(DISTINCT IF(outOfRange,readingId,NULL)) outOfRangePoints
   FROM (
    SELECT productId, readingId, assignmentId, specificationId, lineageIds, maxExcursionSeconds, synthetic, false outOfRange FROM segments
    UNION ALL
    SELECT productId, readingId, assignmentId, specificationId, lineageIds, maxExcursionSeconds, synthetic, outOfRange FROM points
   ) GROUP BY productId
   """).createOrReplaceTempView("evidence");
  // Every source revision the assessment depends on, whether or not it changed a number (an in-range point reading too).
  spark.sql(Exposure.revisions("(SELECT productId, sourceIds FROM intervals UNION ALL SELECT productId, sourceIds FROM points)","productId","events"))
   .createOrReplaceTempView("revisions");
  spark.sql("""
   SELECT productId, sort_array(collect_set(calibrationId)) calibrationIds, max(uncertaintyC) maxUncertaintyC
   FROM calibrated GROUP BY productId
   """).createOrReplaceTempView("calibrations");
  spark.sql("SELECT productId, sort_array(collect_set(custodianId)) custodianIds FROM custodySegments GROUP BY productId")
   .createOrReplaceTempView("custodians");
  spark.sql("""
   SELECT productId, location, status, eventTime FROM (
    SELECT *,row_number() OVER(PARTITION BY productId ORDER BY eventTime DESC,revision DESC,eventId DESC) n
    FROM events WHERE kind='INVENTORY'
   ) WHERE n=1
   """).createOrReplaceTempView("inventory");
  // Ambiguities that touch each unit, reported as flags whatever the outcome. Joined-back sides are re-aliased (Exposure.explained).
  chains.filter("originLevel = 'UNIT'").selectExpr("origin","lineageIds").createOrReplaceTempView("unitPaths");
  lineageAnomalies.selectExpr("explode(eventIds) flaggedId").createOrReplaceTempView("lineageFlags");
  timeline.createOrReplaceTempView("timelineAnomalies");
  Specifications.overlaps(events).filter("resolution = 'UNRESOLVED_SAME_APPROVAL_TIME'").createOrReplaceTempView("specificationTies");
  spark.sql("""
   SELECT DISTINCT u.origin productId, 'LINEAGE' ambiguity FROM (SELECT origin, explode(lineageIds) id FROM unitPaths) u
   JOIN lineageFlags f ON f.flaggedId=u.id
   UNION ALL
   SELECT DISTINCT p.productId, 'ASSIGNMENT' FROM placements p JOIN (
    SELECT e.c, t.a, t.b FROM (SELECT explode(eventIds) tid, startTime a, endTime b FROM timelineAnomalies WHERE anomaly='SENSOR_IN_TWO_CONTAINERS') t
    JOIN (SELECT eventId eid, containerId c FROM events WHERE kind='ASSIGNMENT') e ON e.eid=t.tid) x
    ON x.c=p.containerId AND greatest(p.startTime,x.a) < least(p.endTime,x.b)
   UNION ALL
   SELECT DISTINCT p.productId, 'CUSTODY' FROM placements p
   JOIN (SELECT subjectId c, startTime a, endTime b FROM timelineAnomalies WHERE anomaly='CUSTODY_OVERLAP') x
    ON x.c=p.containerId AND greatest(p.startTime,x.a) < least(p.endTime,x.b)
   UNION ALL
   SELECT DISTINCT p.productId, 'SPECIFICATION' FROM placements p
   JOIN (SELECT productCode c, startTime a, endTime b FROM specificationTies) x
    ON x.c=p.productCode AND greatest(p.startTime,x.a) < least(p.endTime,x.b)
   """).createOrReplaceTempView("ambiguities");
  spark.sql("""
   SELECT productId, bool_or(ambiguity='LINEAGE') ambiguousLineage, bool_or(ambiguity='ASSIGNMENT') ambiguousAssignment,
    bool_or(ambiguity='CUSTODY') ambiguousCustody, bool_or(ambiguity='SPECIFICATION') ambiguousSpecification
   FROM ambiguities GROUP BY productId
   """).createOrReplaceTempView("ambiguous");
  // Excursion under each specification, beside its own allowance. The outcome compares the total with the smallest allowance.
  spark.sql(Exposure.unionSeconds("(SELECT productId, specificationId, startTime, endTime FROM segments WHERE outOfRange)","productId,specificationId"))
   .createOrReplaceTempView("specificationExcursions");
  spark.sql("""
   SELECT g.productId, sort_array(collect_list(named_struct('specificationId',g.specificationId,
    'excursionSeconds',coalesce(x.s,0L),'allowedExcursionSeconds',g.maxExcursionSeconds))) excursionBySpecification
   FROM (SELECT DISTINCT productId, specificationId, maxExcursionSeconds FROM segments
    UNION SELECT productId, specificationId, maxExcursionSeconds FROM points) g
   LEFT JOIN (SELECT productId p, specificationId sid, seconds s FROM specificationExcursions) x ON x.p=g.productId AND x.sid=g.specificationId
   GROUP BY g.productId
   """).createOrReplaceTempView("bySpecification");
  // Inventory age is measured back from the newest source event in the snapshot, so it does not depend on when the batch ran.
  spark.sql("""
   SELECT i.productId, i.inventoryRecordedAt, unix_timestamp(a.asOf)-unix_timestamp(i.inventoryRecordedAt) inventoryAgeSeconds,
    coalesce(m.lastMoved > i.inventoryRecordedAt,false) movedSinceInventory
   FROM (SELECT productId, eventTime inventoryRecordedAt FROM inventory) i
   CROSS JOIN (SELECT max(eventTime) asOf FROM events) a
   LEFT JOIN (SELECT childId u, max(startTime) lastMoved FROM units GROUP BY childId) m ON m.u=i.productId
   """).createOrReplaceTempView("inventoryAge");
  spark.sql("""
   SELECT productId, min(startTime) scopeStart, max(endTime) scopeEnd FROM exposureIntervals WHERE metric='EXPECTED' GROUP BY productId
   """).createOrReplaceTempView("scopes");
  fresh(spark,"metrics","evidence","inventory","custodians","calibrations","revisions","ambiguous","bySpecification","inventoryAge","scopes");
  spark.sql("""
   SELECT m.*, coalesce(e.allowedExcursionSeconds,0L) allowedExcursionSeconds, e.allowedExcursionSeconds allowed,
    coalesce(e.outOfRangePoints,0L) outOfRangePoints, coalesce(e.syntheticThresholds,false) syntheticThresholds,
    e.readingIds,e.assignmentIds,e.specificationIds,e.lineageIds,q.calibrationIds,q.maxUncertaintyC,c.custodianIds,
    coalesce(i.location,'UNKNOWN') location,coalesce(i.status,'UNKNOWN') inventoryStatus,
    transform(v.sources,x -> named_struct('kind',x.kind,'eventId',x.eventId,'revision',x.revision,'payloadHash',x.payloadHash)) sourceRevisions,
    coalesce(a.ambiguousLineage,false) ambiguousLineage, coalesce(a.ambiguousAssignment,false) ambiguousAssignment,
    coalesce(a.ambiguousCustody,false) ambiguousCustody, coalesce(a.ambiguousSpecification,false) ambiguousSpecification,
    b.excursionBySpecification, g.inventoryRecordedAt, g.inventoryAgeSeconds, coalesce(g.movedSinceInventory,false) movedSinceInventory,
    o.scopeStart, o.scopeEnd
   FROM metrics m LEFT JOIN evidence e USING(productId) LEFT JOIN inventory i USING(productId) LEFT JOIN custodians c USING(productId)
    LEFT JOIN calibrations q USING(productId) LEFT JOIN revisions v USING(productId) LEFT JOIN ambiguous a USING(productId)
    LEFT JOIN bySpecification b USING(productId) LEFT JOIN inventoryAge g USING(productId) LEFT JOIN scopes o USING(productId)
   """).createOrReplaceTempView("assessed");
  // Reasons, flags and the outcome come from the policy (Policy.java, docs/detailed-design.md section 5).
  Dataset<Row> gold=spark.sql(Policy.apply("assessed")).withColumn("ruleVersion",lit(RULE_VERSION))
   .drop("allowed").withColumn("assessmentHash",sha2(to_json(struct(col("expectedSeconds"),col("coveredSeconds"),
    col("excursionSeconds"),col("allowedExcursionSeconds"),col("assessment"),col("location"),
    col("inventoryStatus"),col("readingIds"),col("assignmentIds"),col("specificationIds"),col("lineageIds"),
    col("missingCustodySeconds"),col("custodianIds"),col("possibleExcursionSeconds"),col("uncalibratedSeconds"),
    col("unspecifiedSeconds"),col("outOfRangePoints"),col("calibrationIds"),col("maxUncertaintyC"),col("syntheticThresholds"),
    col("assessmentReasons"),col("sourceRevisions"),col("dataQualityFlags"),col("excursionBySpecification"),col("inventoryRecordedAt"),
    col("inventoryAgeSeconds"),col("scopeStart"),col("scopeEnd"),col("assessmentScope"),col("policyVersion"),col("ruleVersion"))),256));
  Dataset<Row> segments=spark.sql(Exposure.revisions("segments",
   "productId,startTime,endTime,lineageIds,assignmentId,sensorId,readingId,specificationId,temperatureC,minC,maxC,outOfRange","events"))
   .withColumn("seconds",expr("unix_timestamp(endTime)-unix_timestamp(startTime)"));
  return new Result(gold,intervals.withColumn("ruleVersion",lit(RULE_VERSION)),segments);
 }
}

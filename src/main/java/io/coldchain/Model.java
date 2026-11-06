package io.coldchain;
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
 public static Dataset<Row> parse(Dataset<Row> bronze) {
  return bronze.withColumn("e",from_json(col("raw"),EVENT)).selectExpr("*","e.*").drop("e")
   .withColumn("valid",coalesce(expr("eventId IS NOT NULL AND length(trim(eventId)) > 0 AND revision >= 1 "
    +"AND eventTime IS NOT NULL AND eventTime <= kafkaTime + INTERVAL 5 MINUTES AND ("
    +"(kind = 'READING' AND sensorId IS NOT NULL AND startTime < endTime AND temperatureC BETWEEN -100 AND 100 "
    +"AND topic = 'coldchain.sensors') OR "
    +"(kind = 'PLACEMENT' AND productId IS NOT NULL AND productCode IS NOT NULL AND lotId IS NOT NULL "
    +"AND containerId IS NOT NULL AND startTime < endTime AND topic = 'coldchain.traceability') OR "
    +"(kind = 'ASSIGNMENT' AND sensorId IS NOT NULL AND containerId IS NOT NULL AND startTime < endTime "
    +"AND topic = 'coldchain.traceability') OR "
    +"(kind = 'SPEC' AND productCode IS NOT NULL AND startTime < endTime AND minC <= maxC "
    +"AND maxExcursionSeconds >= 0 AND topic = 'coldchain.specifications') OR "
    +"(kind = 'INVENTORY' AND productId IS NOT NULL AND location IS NOT NULL AND status IN ('IN_STOCK','DISPATCHED') "
    +"AND topic = 'coldchain.inventory'))"),lit(false)));
 }
 public static Dataset<Row> canonical(Dataset<Row> valid) {
  // The same ID and revision must carry the identical raw contract on every retry.
  Dataset<Row> conflicts=valid.groupBy("eventId","revision").agg(countDistinct("raw").alias("variants"))
   .filter("variants > 1").select("eventId").distinct();
  return valid.join(conflicts,"eventId","left_anti")
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

package io.coldchain;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;

// Sensor-to-container assignments and container custody over time, with their revisions and provenance.
public final class Timelines {
 private Timelines() {}
 static final String OVERLAP=Lineage.OVERLAP;
 static Dataset<Row> contradictions(Dataset<Row> events,String kind,String anomaly,String subject,String other) {
  Dataset<Row> e=events.filter(col("kind").equalTo(kind));
  return e.as("a").join(e.as("b"),expr("a."+subject+" = b."+subject+" AND a."+other+" <> b."+other+" AND a.eventId < b.eventId AND "+OVERLAP))
   .selectExpr("'"+anomaly+"' anomaly","a."+subject+" subjectId","array(a.eventId,b.eventId) eventIds",
    "greatest(a.startTime,b.startTime) startTime","least(a.endTime,b.endTime) endTime");
 }
 // Two sensors on one container are allowed, and both readings count. One sensor cannot measure two containers at once,
 // and a container has one custodian at a time; either case is ambiguous, so it is reported instead of resolved.
 public static Dataset<Row> anomalies(Dataset<Row> events) {
  return contradictions(events,"ASSIGNMENT","SENSOR_IN_TWO_CONTAINERS","sensorId","containerId")
   .unionByName(contradictions(events,"CUSTODY","CUSTODY_OVERLAP","containerId","custodianId"));
 }
 // Current events of one kind that take part in no anomaly; anomalous ones are left out for their whole interval.
 public static Dataset<Row> usable(Dataset<Row> events,String kind,Dataset<Row> anomalies) {
  return events.filter(col("kind").equalTo(kind)).join(anomalies.select(explode(col("eventIds")).alias("eventId")).distinct(),"eventId","left_anti");
 }
 // Every valid revision of each assignment and custody event, once per revision with the coordinates it first arrived at.
 // Conflicting IDs stay withheld. The highest revision is current; only current rows can carry anomalies.
 public static Dataset<Row> history(Dataset<Row> valid,Dataset<Row> anomalies) {
  Dataset<Row> flags=anomalies.select(explode(col("eventIds")).alias("eventId"),col("anomaly"))
   .groupBy("eventId").agg(sort_array(collect_set("anomaly")).alias("flags"));
  return valid.filter("kind IN ('ASSIGNMENT','CUSTODY')").join(Model.conflicts(valid),"eventId","left_anti")
   .withColumn("n",row_number().over(Window.partitionBy("eventId","revision").orderBy("kafkaTime","partition","offset")))
   .filter("n = 1").withColumn("current",col("revision").equalTo(max("revision").over(Window.partitionBy("eventId"))))
   .join(flags,"eventId","left")
   .selectExpr("eventId","revision","kind","current","sensorId","containerId","custodianId","startTime","endTime","eventTime",
    "topic","partition","offset","kafkaTime","sourceSystem","IF(current,coalesce(flags,CAST(array() AS array<string>)),CAST(array() AS array<string>)) anomalies");
 }
 // A revision that changes an assignment's sensor, container or interval affects the units the former revision mapped
 // and the units the revised one maps; both sides are listed, against the current packaging lineage.
 public static Dataset<Row> invalidations(Dataset<Row> history,Dataset<Row> unitContainers) {
  Dataset<Row> changes=history.filter("kind = 'ASSIGNMENT'")
   .withColumn("next",lead(struct(col("revision"),col("sensorId"),col("containerId"),col("startTime"),col("endTime")),1)
    .over(Window.partitionBy("eventId").orderBy("revision")))
   .filter("next IS NOT NULL AND NOT (next.sensorId = sensorId AND next.containerId = containerId "
    +"AND next.startTime = startTime AND next.endTime = endTime)");
  String[] common={"eventId assignmentId","revision fromRevision","next.revision toRevision"};
  Dataset<Row> sides=changes.selectExpr(concat(common,"'FORMER' side","sensorId","containerId","startTime","endTime"))
   .unionByName(changes.selectExpr(concat(common,"'REVISED' side","next.sensorId sensorId","next.containerId containerId",
    "next.startTime startTime","next.endTime endTime")));
  return sides.as("a").join(unitContainers.as("b"),expr("a.containerId = b.containerId AND "+OVERLAP))
   .selectExpr("a.assignmentId","a.fromRevision","a.toRevision","a.side","b.productId","a.sensorId","a.containerId",
    "greatest(a.startTime,b.startTime) startTime","least(a.endTime,b.endTime) endTime");
 }
 static String[] concat(String[] a,String... b) {
  String[] r=java.util.Arrays.copyOf(a,a.length+b.length);System.arraycopy(b,0,r,a.length,b.length);return r;
 }
}

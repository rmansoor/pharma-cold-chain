package io.coldchain;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

// Packaging lineage: which container holds each serialized unit over time, through cartons and pallets.
public final class Lineage {
 private Lineage() {}
 // The contract requires each parent to sit above its child (UNIT < CARTON < PALLET < CONTAINER), so a valid chain has at
 // most 3 hops. A longer chain needs an ID used at two levels, which is reported as LEVEL_MISMATCH; the cap bounds the search.
 static final int MAX_HOPS=8;
 static final String OVERLAP="greatest(a.startTime,b.startTime) < least(a.endTime,b.endTime)";
 // Direct memberships. PLACEMENT puts a unit straight into a container; PACKING nests one level in a higher one.
 public static Dataset<Row> edges(Dataset<Row> events) {
  return events.filter("kind IN ('PLACEMENT','PACKING')").selectExpr("eventId","revision","kind",
   "IF(kind = 'PLACEMENT',productId,childId) childId","IF(kind = 'PLACEMENT','UNIT',childLevel) childLevel",
   "IF(kind = 'PLACEMENT',containerId,parentId) parentId","IF(kind = 'PLACEMENT','CONTAINER',parentLevel) parentLevel",
   "productCode","lotId","startTime","endTime");
 }
 // Every upward chain of memberships that overlap in time, from each child; the interval is their intersection.
 static Dataset<Row> paths(Dataset<Row> edges) {
  Dataset<Row> next=edges.selectExpr("childId","parentId","parentLevel","eventId","startTime","endTime");
  Dataset<Row> frontier=edges.selectExpr("childId origin","childLevel originLevel","productCode","lotId","parentId node","parentLevel level",
   "array(childId,parentId) path","array(eventId) lineageIds","startTime","endTime","false cycle").localCheckpoint(),all=frontier;
  for(int hop=1;hop<MAX_HOPS;hop++) {
   frontier=frontier.filter("NOT cycle").as("a").join(next.as("b"),expr("a.node = b.childId AND "+OVERLAP))
    .selectExpr("a.origin","a.originLevel","a.productCode","a.lotId","b.parentId node","b.parentLevel level",
     "concat(a.path,array(b.parentId)) path","concat(a.lineageIds,array(b.eventId)) lineageIds",
     "greatest(a.startTime,b.startTime) startTime","least(a.endTime,b.endTime) endTime","array_contains(a.path,b.parentId) cycle")
    .localCheckpoint();
   if(frontier.isEmpty())break;
   all=all.unionByName(frontier);
  }
  return all;
 }
 public static Dataset<Row> anomalies(Dataset<Row> edges) {
  // A child cannot be in two different parents at the same instant.
  Dataset<Row> parents=edges.as("a").join(edges.as("b"),expr("a.childId = b.childId AND a.parentId <> b.parentId AND a.eventId < b.eventId AND "+OVERLAP))
   .selectExpr("'CONTRADICTORY_PARENT' anomaly","a.childId subjectId","array(a.eventId,b.eventId) eventIds",
    "greatest(a.startTime,b.startTime) startTime","least(a.endTime,b.endTime) endTime");
  // An ID keeps one packaging level everywhere it appears.
  Dataset<Row> levels=edges.selectExpr("eventId","childId id","childLevel level").unionByName(edges.selectExpr("eventId","parentId id","parentLevel level"))
   .groupBy("id").agg(countDistinct("level").alias("n"),sort_array(collect_set("eventId")).alias("eventIds")).filter("n > 1")
   .selectExpr("'LEVEL_MISMATCH' anomaly","id subjectId","eventIds","CAST(NULL AS timestamp) startTime","CAST(NULL AS timestamp) endTime");
  // An ID that is its own ancestor at some instant. Each member of the loop finds it, so keep one record per set of events.
  Dataset<Row> cycles=paths(edges).filter("cycle AND origin = node").withColumn("eventIds",array_sort(array_distinct(col("lineageIds"))))
   .groupBy("eventIds").agg(min("origin").alias("subjectId"),min("startTime").alias("startTime"),max("endTime").alias("endTime"))
   .selectExpr("'CYCLE' anomaly","subjectId","eventIds","startTime","endTime");
  return parents.unionByName(levels).unionByName(cycles);
 }
 // Each direct membership with the anomalies it takes part in; an empty list means it is used for lineage.
 public static Dataset<Row> memberships(Dataset<Row> edges,Dataset<Row> anomalies) {
  Dataset<Row> flags=anomalies.select(explode(col("eventIds")).alias("eventId"),col("anomaly"))
   .groupBy("eventId").agg(sort_array(collect_set("anomaly")).alias("anomalies"));
  return edges.join(flags,"eventId","left").withColumn("anomalies",coalesce(col("anomalies"),array().cast("array<string>")));
 }
 // Container intervals per serialized unit, built only from memberships without anomalies, with the path and source events.
 public static Dataset<Row> unitContainers(Dataset<Row> edges,Dataset<Row> anomalies) {
  Dataset<Row> flagged=anomalies.select(explode(col("eventIds")).alias("eventId")).distinct();
  return paths(edges.join(flagged,"eventId","left_anti")).filter("originLevel = 'UNIT' AND level = 'CONTAINER' AND NOT cycle")
   .selectExpr("origin productId","productCode","lotId","node containerId","startTime","endTime","path","lineageIds");
 }
}

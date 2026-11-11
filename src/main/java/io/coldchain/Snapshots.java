package io.coldchain;
import java.util.*;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;

// Gold publication. A snapshot's assessment history and interval evidence are written first and its manifest row in
// gold/commits last; a snapshot without a manifest row does not exist for readers. Readers pin one committed snapshot and
// read every Gold table at its snapshotId and at the Delta version its manifest recorded.
public final class Snapshots {
 private Snapshots() {}
 static final String COMMITS="gold/commits",HISTORY="gold/assessment_history",INTERVALS="gold/exposure_intervals",IMPACTS="gold/snapshot_impacts",
  SEGMENTS="gold/exposure_segments";
 // A table exists once its log holds a commit. DeltaTable.isDeltaTable is not used for this: polled by a reader while another
 // process writes the table's first commit, it caches "no table" and keeps returning false until something reads the table.
 public static boolean exists(SparkSession s,String base,String table) {
  try {
   org.apache.hadoop.fs.Path log=new org.apache.hadoop.fs.Path(base+"/"+table+"/_delta_log");
   org.apache.hadoop.fs.FileSystem fs=log.getFileSystem(s.sparkContext().hadoopConfiguration());
   if(!fs.exists(log))return false;
   for(org.apache.hadoop.fs.FileStatus f:fs.listStatus(log))
    if(f.getPath().getName().matches("\\d{20}\\.json|_last_checkpoint"))return true;
   return false;
  } catch(java.io.IOException e){throw new java.io.UncheckedIOException(e);}
 }
 static Dataset<Row> table(SparkSession s,String base,String table){return s.read().format("delta").load(base+"/"+table);}
 // The newest committed snapshot, or null before the first commit.
 public static Row latest(SparkSession s,String base) {
  if(!exists(s,base,COMMITS))return null;
  List<Row> rows=table(s,base,COMMITS).orderBy(col("snapshotId").desc()).limit(1).collectAsList();
  return rows.isEmpty()?null:rows.get(0);
 }
 // Gold history is a change log: a snapshot writes rows only for the units it recomputed, and a tombstone (removed) for a unit
 // that no longer exists. The state as of a snapshot is each unit's newest row among committed snapshots up to it, unless that
 // row is a tombstone.
 static Dataset<Row> resolve(Dataset<Row> history,Dataset<Row> snapshots,long upTo) {
  if(!Arrays.asList(history.columns()).contains("removed"))history=history.withColumn("removed",lit(false));
  Dataset<Row> rows=history.filter(col("snapshotId").leq(upTo)).join(snapshots.select(col("snapshotId").as("snapshotId")).distinct(),"snapshotId","left_semi");
  return rows.withColumn("n",row_number().over(Window.partitionBy("productId").orderBy(col("snapshotId").desc())))
   .filter("n = 1 AND NOT coalesce(removed,false)").drop("n");
 }
 // Stored assessments in the state of a committed snapshot, without the dataAsOf-dependent fields.
 static Dataset<Row> state(SparkSession s,String base,Row commit) {
  long id=commit.getAs("snapshotId");
  return resolve(at(s,base,commit,HISTORY),table(s,base,COMMITS).filter(col("snapshotId").leq(id)),id);
 }
 // The assessments a reader sees for a committed snapshot: its state, with inventory age and the flags that depend on it
 // measured back from the snapshot's dataAsOf. Commits before SID-23 stored those fields in every row.
 public static Dataset<Row> assessments(SparkSession s,String base,Row commit) {
  Dataset<Row> state=state(s,base,commit);
  return incremental(commit)?Policy.asOf(state,lit(commit.<java.sql.Timestamp>getAs("dataAsOf"))):state;
 }
 static boolean incremental(Row commit){return Arrays.asList(commit.schema().fieldNames()).contains("recomputeMode")&&!commit.isNullAt(commit.fieldIndex("recomputeMode"));}
 // Interval evidence of the units in the snapshot's state: for each unit, the intervals of the snapshot its row came from.
 public static Dataset<Row> intervals(SparkSession s,String base,Row commit) {
  Dataset<Row> rows=state(s,base,commit).select(col("productId").as("productId"),col("snapshotId").as("snapshotId"));
  return at(s,base,commit,INTERVALS).join(rows,new String[]{"productId","snapshotId"},"left_semi");
 }
 // The measured segments of the units in the snapshot's state, from the snapshot that last recomputed each unit.
 public static Dataset<Row> segments(SparkSession s,String base,Row commit) {
  Dataset<Row> rows=state(s,base,commit).select(col("productId").as("productId"),col("snapshotId").as("snapshotId"));
  return at(s,base,commit,SEGMENTS).join(rows,new String[]{"productId","snapshotId"},"left_semi");
 }
 // Every assessment of every committed snapshot (tombstones excluded), for readers that compare snapshots.
 public static Dataset<Row> committedHistory(SparkSession s,String base) {
  Dataset<Row> h=table(s,base,HISTORY);
  if(Arrays.asList(h.columns()).contains("removed"))h=h.filter("NOT coalesce(removed,false)");
  return h.join(table(s,base,COMMITS).select("snapshotId"),"snapshotId","left_semi");
 }
 static Dataset<Row> at(SparkSession s,String base,Row commit,String table) {
  DataFrameReader r=s.read().format("delta");
  Long version=recorded(commit,table);
  // Commits written before SID-22 record no versions; the snapshotId filters still select only committed rows.
  if(version!=null)r=r.option("versionAsOf",version);
  return r.load(base+"/"+table);
 }
 static Long recorded(Row commit,String table) {
  if(!Arrays.asList(commit.schema().fieldNames()).contains("tableVersions")||commit.isNullAt(commit.fieldIndex("tableVersions")))return null;
  for(Row v:commit.<Row>getList(commit.fieldIndex("tableVersions")))if(table.equals(v.getAs("table")))return v.getAs("version");
  return null;
 }
 // Rows of a Gold table whose snapshot has no manifest: left by a batch that stopped before publishing. Never read.
 public static Dataset<Row> uncommitted(SparkSession s,String base,String table) {
  if(!exists(s,base,table))return null;
  Dataset<Row> rows=table(s,base,table);
  return exists(s,base,COMMITS)?rows.join(table(s,base,COMMITS).select("snapshotId"),"snapshotId","left_anti"):rows;
 }
 // True when snapshot id is already published: a replay after the manifest must publish nothing. Batch IDs are only unique
 // within one checkpoint lineage, so a manifest from another lineage means the checkpoint was replaced under retained Gold.
 static boolean committed(SparkSession s,String base,long id,String lineage) {
  if(!exists(s,base,COMMITS))return false;
  Dataset<Row> commits=table(s,base,COMMITS);
  if(Arrays.asList(commits.columns()).contains("lineageId")) {
   List<Row> other=commits.filter(col("lineageId").isNotNull().and(col("lineageId").notEqual(lineage))).select("lineageId").distinct().collectAsList();
   if(!other.isEmpty())throw new IllegalStateException("gold/commits was published by checkpoint lineage "+other.get(0).getString(0)
    +", not "+lineage+": a new checkpoint needs a new data namespace, or its batch IDs would collide with published snapshots");
  }
  return commits.filter(col("snapshotId").equalTo(id)).count()>0;
 }
 // Before writing snapshot id again, remove what an earlier attempt left without a manifest, so the retry's rows are the
 // only ones and a change between attempts cannot leave stale rows behind.
 static void discardUncommitted(SparkSession s,String base,long id) {
  for(String t:List.of(HISTORY,INTERVALS,SEGMENTS,IMPACTS,Outbox.OUTBOX))if(exists(s,base,t))DeltaTable.forPath(s,base+"/"+t).delete(col("snapshotId").equalTo(id));
 }
 // Content digests of a snapshot's state, covering content only (not times or identities), so an incremental snapshot can be
 // compared with a full recomputation or a replay of the same source history.
 public static Dataset<Row> digests(Dataset<Row> state,Dataset<Row> intervals) {
  Dataset<Row> evidence=state.selectExpr("explode(sourceRevisions) x").selectExpr("concat_ws(':',x.kind,x.eventId,x.revision,x.payloadHash) r")
   .agg(size(collect_set("r")).cast("long").alias("evidenceRevisions"),sha2(array_join(array_sort(collect_set("r")),"\n"),256).alias("evidenceDigest"));
  Dataset<Row> assessments=state.agg(count(lit(1)).alias("products"),
   sha2(array_join(array_sort(collect_list(concat_ws(":",col("productId"),col("assessmentHash")))),"\n"),256).alias("assessmentsDigest"));
  Dataset<Row> evidenceIntervals=intervals.agg(sha2(array_join(array_sort(collect_list(concat_ws("|",col("productId"),col("metric"),
   col("startTime").cast("string"),col("endTime").cast("string"),col("seconds").cast("string"),to_json(col("sources"))))),"\n"),256).alias("intervalsDigest"));
  return evidence.crossJoin(assessments).crossJoin(evidenceIntervals);
 }
 // Why the next snapshot must be recomputed in full, or null when the previous snapshot can be updated incrementally: its
 // state was produced by the same rules, policy, code and configuration from a recorded canonical version.
 static String fullReason(Row previous,String code,boolean allowSynthetic) {
  if(previous==null)return "NO_PREVIOUS_SNAPSHOT";
  if(!incremental(previous)||recorded(previous,"silver/events_current")==null)return "PREVIOUS_SNAPSHOT_FORMAT";
  if(!Model.RULE_VERSION.equals(previous.getAs("ruleVersion")))return "RULE_VERSION_CHANGED";
  if(!Policy.VERSION.equals(previous.getAs("policyVersion")))return "POLICY_VERSION_CHANGED";
  if(!Arrays.asList(previous.schema().fieldNames()).contains("rowVersion")||!Model.ROW_VERSION.equals(previous.getAs("rowVersion")))return "ROW_VERSION_CHANGED";
  if(!code.equals(previous.getAs("codeVersion")))return "CODE_VERSION_CHANGED";
  if(allowSynthetic!=previous.<Boolean>getAs("allowSyntheticSpecifications"))return "CONFIGURATION_CHANGED";
  return null;
 }
 // The manifest row: identity, versions, how the snapshot and its Silver were computed, source high-watermarks, input and output table
 // versions and the digests of its state.
 static Dataset<Row> manifest(long id,String lineage,String run,String code,boolean allowSynthetic,String mode,String reason,
   String silverMode,String silverReason,long changed,long recomputed,Dataset<Row> source,Dataset<Row> current,Dataset<Row> digests,Map<String,Long> versions) {
  StringJoiner tables=new StringJoiner(",","array(",")");
  versions.forEach((t,v)->tables.add("named_struct('table','"+t+"','version',CAST("+v+" AS bigint))"));
  Dataset<Row> marks=source.groupBy("topic","partition").agg(max("offset").alias("maxOffset"),max("kafkaTime").alias("maxKafkaTime"))
   .agg(sort_array(collect_list(struct("topic","partition","maxOffset","maxKafkaTime"))).alias("sourceHighWatermarks"));
  Dataset<Row> asOf=current.agg(max("eventTime").alias("dataAsOf"));
  return marks.crossJoin(digests).crossJoin(asOf).select(lit(id).alias("snapshotId"),current_timestamp().alias("committedAt"),
   col("products"),lit(lineage).alias("lineageId"),lit(run).alias("runId"),lit(code).alias("codeVersion"),lit(Model.RULE_VERSION).alias("ruleVersion"),
   lit(Policy.VERSION).alias("policyVersion"),lit(Model.ROW_VERSION).alias("rowVersion"),lit(allowSynthetic).alias("allowSyntheticSpecifications"),lit(mode).alias("recomputeMode"),
   lit(reason).cast("string").alias("recomputeReason"),lit(silverMode).alias("silverMode"),lit(silverReason).cast("string").alias("silverReason"),lit(changed).alias("changedEvents"),lit(recomputed).alias("recomputedProducts"),
   col("dataAsOf"),col("sourceHighWatermarks"),expr(tables.toString()).alias("tableVersions"),
   col("evidenceRevisions"),col("evidenceDigest"),col("assessmentsDigest"),col("intervalsDigest"));
 }
}

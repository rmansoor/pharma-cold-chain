package io.coldchain;
import java.util.*;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

// Gold publication. A snapshot's assessment history and interval evidence are written first and its manifest row in
// gold/commits last; a snapshot without a manifest row does not exist for readers. Readers pin one committed snapshot and
// read every Gold table at its snapshotId and at the Delta version its manifest recorded.
public final class Snapshots {
 private Snapshots() {}
 static final String COMMITS="gold/commits",HISTORY="gold/assessment_history",INTERVALS="gold/exposure_intervals";
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
 public static Dataset<Row> assessments(SparkSession s,String base,Row commit){return at(s,base,commit,HISTORY);}
 public static Dataset<Row> intervals(SparkSession s,String base,Row commit){return at(s,base,commit,INTERVALS);}
 // Every assessment of every committed snapshot, for readers that compare snapshots.
 public static Dataset<Row> committedHistory(SparkSession s,String base) {
  return table(s,base,HISTORY).join(table(s,base,COMMITS).select("snapshotId"),"snapshotId","left_semi");
 }
 static Dataset<Row> at(SparkSession s,String base,Row commit,String table) {
  DataFrameReader r=s.read().format("delta");
  Long version=recorded(commit,table);
  // Commits written before SID-22 record no versions; the snapshotId filter alone still selects only their rows.
  if(version!=null)r=r.option("versionAsOf",version);
  return r.load(base+"/"+table).filter(col("snapshotId").equalTo(commit.<Long>getAs("snapshotId")));
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
  for(String t:List.of(HISTORY,INTERVALS))if(exists(s,base,t))DeltaTable.forPath(s,base+"/"+t).delete(col("snapshotId").equalTo(id));
 }
 // The manifest row: identity, versions, source high-watermarks, input and output table versions and content digests.
 // The digests cover content only (not times or identities), so a retried snapshot can be compared with a clean one.
 static Dataset<Row> manifest(SparkSession s,long id,String lineage,String run,String code,Dataset<Row> assessed,Dataset<Row> source,
   Dataset<Row> current,Map<String,Long> versions) {
  StringJoiner tables=new StringJoiner(",","array(",")");
  versions.forEach((t,v)->tables.add("named_struct('table','"+t+"','version',CAST("+v+" AS bigint))"));
  Dataset<Row> marks=source.groupBy("topic","partition").agg(max("offset").alias("maxOffset"),max("kafkaTime").alias("maxKafkaTime"))
   .agg(sort_array(collect_list(struct("topic","partition","maxOffset","maxKafkaTime"))).alias("sourceHighWatermarks"));
  Dataset<Row> evidence=assessed.selectExpr("explode(sourceRevisions) x").selectExpr("concat_ws(':',x.kind,x.eventId,x.revision,x.payloadHash) r")
   .agg(size(collect_set("r")).cast("long").alias("evidenceRevisions"),sha2(array_join(array_sort(collect_set("r")),"\n"),256).alias("evidenceDigest"));
  Dataset<Row> digest=assessed.agg(count(lit(1)).alias("products"),
   sha2(array_join(array_sort(collect_list(concat_ws(":",col("productId"),col("assessmentHash")))),"\n"),256).alias("assessmentsDigest"));
  Dataset<Row> asOf=current.agg(max("eventTime").alias("dataAsOf"));
  return marks.crossJoin(evidence).crossJoin(digest).crossJoin(asOf).select(lit(id).alias("snapshotId"),current_timestamp().alias("committedAt"),
   col("products"),lit(lineage).alias("lineageId"),lit(run).alias("runId"),lit(code).alias("codeVersion"),lit(Model.RULE_VERSION).alias("ruleVersion"),
   lit(Policy.VERSION).alias("policyVersion"),col("dataAsOf"),col("sourceHighWatermarks"),expr(tables.toString()).alias("tableVersions"),
   col("evidenceRevisions"),col("evidenceDigest"),col("assessmentsDigest"));
 }
}

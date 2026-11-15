package io.coldchain;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

// Retention of superseded Gold history. A plan (OPERATOR) lists the snapshots committed before the horizon, except the newest,
// and the rows of theirs that no unit's current row needs: superseded assessments with their intervals, segments and review
// items, and their impact records. Quality approves the plan by its ID (Governance approve RETENTION <planId>), never its own
// planner. Execution (OPERATOR) deletes exactly the planned rows and marks those snapshots purged, so readers refuse them
// (Snapshots); a plan that no longer matches the lake is refused. The newest state, its digests and every Bronze and Silver
// record are untouched. Deletion is logical: Delta keeps removed files for time travel until a VACUUM, which pinned readers
// make unsafe here (docs/decisions-and-operations.md, Retention and deletion policy).
// Usage: Retention plan <days to keep> | Retention execute <planId>. The caller is COLDCHAIN_PRINCIPAL.
public final class Retention {
 private Retention() {}
 static final String PLANS="audit/retention_plans";
 // Tables purged per unit row (productId, snapshotId), and per snapshot.
 static final List<String> PER_UNIT=List.of(Snapshots.HISTORY,Snapshots.INTERVALS,Snapshots.SEGMENTS,Outbox.OUTBOX),PER_SNAPSHOT=List.of(Snapshots.IMPACTS);
 record Plan(String planId,List<Long> snapshots,Map<String,Long> rows){}
 static String sha256(String text) {
  try{return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));}
  catch(java.security.NoSuchAlgorithmException e){throw new IllegalStateException(e);}
 }

 // Each unit's newest row among committed snapshots, tombstones included: the rows retention keeps.
 static Dataset<Row> kept(SparkSession s,String base) {
  Dataset<Row> history=Snapshots.table(s,base,Snapshots.HISTORY).join(Snapshots.table(s,base,Snapshots.COMMITS).select("snapshotId"),"snapshotId","left_semi");
  return history.withColumn("n",row_number().over(Window.partitionBy("productId").orderBy(col("snapshotId").desc())))
   .filter("n = 1").select(col("productId").as("productId"),col("snapshotId").as("snapshotId"));
 }
 // The rows of table that a purge of these snapshots deletes.
 static Dataset<Row> doomed(SparkSession s,String base,String table,List<Long> snapshots,Dataset<Row> kept) {
  Dataset<Row> rows=Snapshots.table(s,base,table).filter(col("snapshotId").isin(snapshots.toArray()));
  return PER_SNAPSHOT.contains(table)?rows:rows.join(kept,new String[]{"productId","snapshotId"},"left_anti");
 }
 static Plan compute(SparkSession s,String base,List<Long> snapshots) {
  Dataset<Row> kept=kept(s,base).localCheckpoint();
  Map<String,Long> rows=new TreeMap<>();
  for(String t:PER_UNIT)if(Snapshots.exists(s,base,t))rows.put(t,snapshots.isEmpty()?0:doomed(s,base,t,snapshots,kept).count());
  for(String t:PER_SNAPSHOT)if(Snapshots.exists(s,base,t))rows.put(t,snapshots.isEmpty()?0:doomed(s,base,t,snapshots,kept).count());
  return new Plan(null,snapshots,rows);
 }
 // Snapshots committed before now - keep, other than the newest, not purged yet.
 static List<Long> candidates(SparkSession s,String base,Duration keep,long now) {
  Row newest=Snapshots.latest(s,base);
  if(newest==null)return List.of();
  Dataset<Row> commits=Snapshots.table(s,base,Snapshots.COMMITS);
  if(Arrays.asList(commits.columns()).contains("purgedAt"))commits=commits.filter("purgedAt IS NULL");
  return commits.filter(col("committedAt").lt(lit(new Timestamp(now-keep.toMillis())))).filter(col("snapshotId").notEqual(newest.<Long>getAs("snapshotId")))
   .select("snapshotId").orderBy("snapshotId").as(Encoders.LONG()).collectAsList();
 }
 static Plan plan(SparkSession s,String base,String planner,Duration keep,long now) {
  Governance.require(s,planner,Governance.Role.OPERATOR,"RETENTION_PLAN",base);
  Plan content=compute(s,base,candidates(s,base,keep,now));
  // Unique per plan, so an approval names one planner's plan even when two plans list the same rows.
  Plan p=new Plan(sha256(base+"|"+content.snapshots()+"|"+content.rows()+"|"+planner+"|"+now+"|"+UUID.randomUUID()).substring(0,16),content.snapshots(),content.rows());
  s.createDataFrame(List.of(RowFactory.create(p.planId(),base,planner,new Timestamp(now),keep.toDays(),p.snapshots().toArray(new Long[0]),p.rows().toString(),null,null)),
   new StructType().add("planId","string").add("lake","string").add("planner","string").add("plannedAt","timestamp").add("keepDays","long")
    .add("snapshots",DataTypes.createArrayType(DataTypes.LongType)).add("rows","string").add("executedBy","string").add("executedAt","timestamp"))
   .write().format("delta").mode("append").save(Governance.dir()+"/"+PLANS);
  Governance.audit(s,planner,Governance.Role.OPERATOR,"RETENTION_PLAN",p.planId(),"ALLOWED","keep "+keep.toDays()+" days: snapshots "+p.snapshots()+", rows "+p.rows());
  return p;
 }
 static Row stored(SparkSession s,String planId) {
  if(!Snapshots.exists(s,Governance.dir(),PLANS))return null;
  List<Row> r=Snapshots.table(s,Governance.dir(),PLANS).filter(col("planId").equalTo(planId)).collectAsList();
  return r.isEmpty()?null:r.get(0);
 }
 static String planner(SparkSession s,String planId){Row r=stored(s,planId);return r==null?null:r.getAs("planner");}
 // Deletes the planned rows; returns how many. Refused, and audited, unless quality approved this exact plan and it still
 // matches the lake.
 static long execute(SparkSession s,String base,String executor,String planId) {
  Governance.require(s,executor,Governance.Role.OPERATOR,"RETENTION_PURGE",planId);
  Row stored=stored(s,planId);
  String refusal=stored==null?"no such plan":stored.getAs("executedAt")!=null?"plan already executed":!base.equals(stored.getAs("lake"))?"plan belongs to another lake":null;
  Row approval=refusal==null?Governance.approval(s,"RETENTION",planId):null;
  if(refusal==null&&approval==null)refusal="plan not approved by quality";
  if(refusal==null&&approval.getAs("approvedBy").equals(executor))refusal="the approver may not execute the plan";
  Plan now=refusal==null?compute(s,base,stored.<Long>getList(stored.fieldIndex("snapshots"))):null;
  if(refusal==null&&!now.rows().toString().equals(stored.getAs("rows")))refusal="plan no longer matches the lake (now "+now.rows()+"); plan again";
  if(refusal!=null) {
   Governance.audit(s,executor,Governance.Role.OPERATOR,"RETENTION_PURGE",planId,"DENIED",refusal);
   throw new ImpactQueries.AccessDenied("retention plan "+planId+" refused: "+refusal);
  }
  Dataset<Row> kept=kept(s,base).localCheckpoint();long deleted=0;
  for(String t:PER_UNIT)if(Snapshots.exists(s,base,t)) {
   Dataset<Row> keys=doomed(s,base,t,now.snapshots(),kept).select("productId","snapshotId").distinct().localCheckpoint();
   if(!keys.isEmpty())DeltaTable.forPath(s,base+"/"+t).as("t").merge(keys.as("k"),"t.productId = k.productId AND t.snapshotId = k.snapshotId").whenMatched().delete().execute();
  }
  for(String t:PER_SNAPSHOT)if(Snapshots.exists(s,base,t))DeltaTable.forPath(s,base+"/"+t).delete(col("snapshotId").isin(now.snapshots().toArray()));
  deleted=now.rows().values().stream().mapToLong(Long::longValue).sum();
  // Purged snapshots keep their manifest (what was published, and when) and are marked so readers refuse them.
  if(!Arrays.asList(Snapshots.table(s,base,Snapshots.COMMITS).columns()).contains("purgedAt"))
   s.sql("ALTER TABLE delta.`"+base+"/"+Snapshots.COMMITS+"` ADD COLUMNS (purgedAt timestamp)");
  if(!now.snapshots().isEmpty())DeltaTable.forPath(s,base+"/"+Snapshots.COMMITS).update(col("snapshotId").isin(now.snapshots().toArray()),Map.of("purgedAt",current_timestamp()));
  DeltaTable.forPath(s,Governance.dir()+"/"+PLANS).update(col("planId").equalTo(planId),Map.of("executedBy",lit(executor),"executedAt",current_timestamp()));
  Governance.audit(s,executor,Governance.Role.OPERATOR,"RETENTION_PURGE",planId,"DONE","approval "+approval.getAs("approvalId")+" by "+approval.getAs("approvedBy")
   +": snapshots "+now.snapshots()+", rows "+now.rows());
  return deleted;
 }
 public static void main(String[] args) {
  SparkSession s=Pipeline.session("Cold-chain retention");s.sparkContext().setLogLevel("ERROR");
  try {
   if(args.length==2&&args[0].equals("plan")) {
    Plan p=plan(s,Pipeline.DATA,Governance.caller(),Duration.ofDays(Long.parseLong(args[1])),System.currentTimeMillis());
    System.out.println("PLAN "+p.planId()+" snapshots "+p.snapshots()+" rows "+p.rows());
   } else if(args.length==2&&args[0].equals("execute"))
    System.out.println("PURGED plan "+args[1]+": "+execute(s,Pipeline.DATA,Governance.caller(),args[1])+" rows");
   else throw new IllegalArgumentException("usage: Retention plan <days to keep> | Retention execute <planId>");
  } catch(ImpactQueries.AccessDenied e){System.out.println("DENIED: "+e.getMessage());s.stop();System.exit(3);}
  s.stop();
 }
}

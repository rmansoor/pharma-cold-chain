package io.coldchain;
import java.nio.file.*;
import java.util.*;
import io.delta.tables.DeltaTable;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
// Runs the real Pipeline.process against temporary lakes, with failures injected between the Gold writes.
class PublicationTest {
 static SparkSession s;static String data;static String clean;static final String LINEAGE="lineage-1";
 @BeforeAll static void start()throws Exception {
  s=Pipeline.session("cold-chain-publication-tests");s.sparkContext().setLogLevel("ERROR");s.conf().set(Specifications.ALLOW_SYNTHETIC,true);
  data=Pipeline.DATA;Pipeline.halt=false;
 }
 @AfterAll static void stop(){Pipeline.DATA=data;Pipeline.failAt=null;Pipeline.halt=true;s.stop();}
 // A Kafka micro-batch: partition 0 of each event's topic, offsets from `first`.
 static Dataset<Row> batch(long first,List<Map<String,Object>> events)throws Exception {
  List<Row> rows=new ArrayList<>();long offset=first;
  for(var e:events)rows.add(RowFactory.create(Simulator.topic((String)e.get("kind")),offset++,Simulator.JSON.writeValueAsString(e)));
  return s.createDataFrame(rows,new org.apache.spark.sql.types.StructType().add("topic","string").add("offset","long").add("value","string"))
   .selectExpr("topic","0 partition","offset","current_timestamp() timestamp","CAST(NULL AS binary) key","CAST(value AS binary) value",
    "CAST(array() AS array<struct<key:string,value:binary>>) headers");
 }
 static Dataset<Row> initial()throws Exception{return batch(0,Simulator.initial());}
 // The demo's correction: the sensor was in BOX-B.
 static Dataset<Row> correction()throws Exception {
  Map<String,Object> c=new LinkedHashMap<>(Simulator.initial().get(3));c.put("revision",2);c.put("containerId","BOX-B");
  return batch(Simulator.initial().size(),List.of(c));
 }
 static void run(String dir,Dataset<Row> batch,long id,String lineage,String failAt) {
  Pipeline.DATA=dir;Pipeline.failAt=failAt;
  try{Pipeline.process(batch,id,lineage);}finally{Pipeline.failAt=null;}
 }
 static String lake(String name)throws Exception{return Files.createTempDirectory("cold-chain-"+name).toString();}
 // Snapshots 0 and 1 written without any failure: the reference every retry must reproduce.
 static synchronized String clean()throws Exception {
  if(clean==null){String d=lake("clean");run(d,initial(),0,LINEAGE,null);run(d,correction(),1,LINEAGE,null);clean=d;}
  return clean;
 }
 static Row latest(String dir){return Snapshots.latest(s,dir);}
 static long rows(String dir,String table,long snapshot){return s.read().format("delta").load(dir+"/"+table).filter(col("snapshotId").equalTo(snapshot)).count();}
 @Test void partialSnapshotsStayInvisibleAndRetriesAreExact()throws Exception {
  String ref=clean(),d=lake("faulty");
  run(d,initial(),0,LINEAGE,null);
  Row first=latest(d);
  // Stopped after the interval evidence, then after the assessment history: either way readers still see snapshot 0 only.
  for(String stage:List.of("intervals","history")) {
   IllegalStateException e=assertThrows(IllegalStateException.class,()->run(d,correction(),1,LINEAGE,stage+":1"));
   assertTrue(e.getMessage().contains("after "+stage),e.getMessage());
   Row pinned=latest(d);
   assertEquals(0L,(long)pinned.getAs("snapshotId"),stage);
   assertEquals(first.<String>getAs("assessmentsDigest"),pinned.<String>getAs("assessmentsDigest"),stage);
   assertEquals(List.of(0L),Snapshots.assessments(s,d,pinned).select("snapshotId").distinct().as(Encoders.LONG()).collectAsList(),stage);
   assertTrue(Snapshots.uncommitted(s,d,Snapshots.INTERVALS).count()>0,stage+" left interval rows without a manifest");
  }
  assertEquals(3L,Snapshots.uncommitted(s,d,Snapshots.HISTORY).count(),"the history attempt wrote all three units");
  // Plant a row no retry would produce, as if an earlier attempt had run other code: the retry must remove it.
  Dataset<Row> stale=s.read().format("delta").load(d+"/"+Snapshots.HISTORY).filter("snapshotId = 1").limit(1).withColumn("productId",lit("UNIT-STALE"));
  stale.write().format("delta").mode("append").save(d+"/"+Snapshots.HISTORY);
  run(d,correction(),1,LINEAGE,null);
  Row retried=latest(d),reference=latest(ref);
  assertEquals(1L,(long)retried.getAs("snapshotId"));
  assertEquals(0L,Snapshots.uncommitted(s,d,Snapshots.HISTORY).count());
  assertEquals(3L,rows(d,Snapshots.HISTORY,1),"no duplicate and no stale row");
  assertEquals(rows(ref,Snapshots.INTERVALS,1),rows(d,Snapshots.INTERVALS,1));
  for(String digest:List.of("assessmentsDigest","evidenceDigest","evidenceRevisions","products"))
   assertEquals(reference.<Object>getAs(digest),retried.<Object>getAs(digest),digest+" matches the clean run");
  // Replaying the batch after its manifest (Spark had not yet recorded it as done) publishes nothing.
  long history=s.read().format("delta").load(d+"/"+Snapshots.HISTORY).count(),intervals=s.read().format("delta").load(d+"/"+Snapshots.INTERVALS).count();
  long historyVersion=Pipeline.version(s,d+"/"+Snapshots.HISTORY),intervalsVersion=Pipeline.version(s,d+"/"+Snapshots.INTERVALS);
  run(d,correction(),1,LINEAGE,null);
  // Not even rewritten: deleting and rewriting published rows would briefly hide them from readers.
  assertEquals(historyVersion,Pipeline.version(s,d+"/"+Snapshots.HISTORY));assertEquals(intervalsVersion,Pipeline.version(s,d+"/"+Snapshots.INTERVALS));
  assertEquals(2L,s.read().format("delta").load(d+"/"+Snapshots.COMMITS).count());
  assertEquals(history,s.read().format("delta").load(d+"/"+Snapshots.HISTORY).count());
  assertEquals(intervals,s.read().format("delta").load(d+"/"+Snapshots.INTERVALS).count());
  assertEquals(retried.<java.sql.Timestamp>getAs("committedAt"),latest(d).<java.sql.Timestamp>getAs("committedAt"));
  // A new checkpoint lineage would restart batch IDs against published snapshots: publication is refused.
  IllegalStateException refused=assertThrows(IllegalStateException.class,()->run(d,batch(100,List.of(Simulator.initial().get(6))),2,"lineage-2",null));
  assertTrue(refused.getMessage().contains("lineage-1"),refused.getMessage());
  assertEquals(1L,(long)latest(d).getAs("snapshotId"));assertEquals(0L,Snapshots.uncommitted(s,d,Snapshots.HISTORY).count());
 }
 @Test void readersSeeATableWrittenByAnotherProcess()throws Exception {
  // A reader polled while another process wrote the first commit of gold/commits. The commit is copied into place here, as a
  // write from outside this JVM; DeltaTable.isDeltaTable keeps its cached answer, Snapshots.exists does not.
  String root=lake("foreign"),source=root+"/source",target=root+"/gold/commits";
  s.range(1).selectExpr("id snapshotId").write().format("delta").save(source);
  Files.createDirectories(Path.of(target,"_delta_log"));
  assertFalse(Snapshots.exists(s,root,Snapshots.COMMITS));assertFalse(DeltaTable.isDeltaTable(s,target));
  try(var files=Files.walk(Path.of(source))) {
   for(Path f:(Iterable<Path>)files::iterator) {
    Path t=Path.of(target).resolve(Path.of(source).relativize(f));
    if(Files.isDirectory(f))Files.createDirectories(t);else Files.copy(f,t);
   }
  }
  assertFalse(DeltaTable.isDeltaTable(s,target),"the cached answer this guards against");
  assertTrue(Snapshots.exists(s,root,Snapshots.COMMITS));assertEquals(0L,(long)latest(root).getAs("snapshotId"));
 }
 @Test void manifestRecordsIdentityVersionsWatermarksAndEvidence()throws Exception {
  String d=clean();
  Row commit=latest(d);
  assertEquals(LINEAGE,commit.getAs("lineageId"));assertEquals(Pipeline.RUN_ID,commit.getAs("runId"));assertEquals("dev",commit.getAs("codeVersion"));
  assertEquals(Model.RULE_VERSION,commit.getAs("ruleVersion"));assertEquals(Policy.VERSION,commit.getAs("policyVersion"));
  assertEquals("2026-01-01T12:00:00Z",commit.<java.sql.Timestamp>getAs("dataAsOf").toInstant().toString());
  // High-watermarks per topic partition after both batches: 13 initial events (offsets 0-12) and the correction (13).
  List<String> marks=new ArrayList<>();
  for(Row m:commit.<Row>getList(commit.fieldIndex("sourceHighWatermarks")))marks.add(m.getAs("topic")+"/"+m.getAs("partition")+"@"+m.getAs("maxOffset"));
  assertEquals(List.of("coldchain.inventory/0@5","coldchain.sensors/0@12","coldchain.specifications/0@0","coldchain.traceability/0@13"),marks);
  // Recorded versions are the versions written: reading at them gives exactly the published rows.
  for(String t:List.of("bronze/events","silver/events_current","silver/exposure_segments",Snapshots.HISTORY,Snapshots.INTERVALS,"gold/assessment_policies"))
   assertNotNull(Snapshots.recorded(commit,t),t);
  assertEquals(Pipeline.version(s,d+"/bronze/events"),(long)Snapshots.recorded(commit,"bronze/events"));
  Row earlier=s.read().format("delta").load(d+"/"+Snapshots.COMMITS).filter("snapshotId = 0").first();
  assertTrue(Snapshots.recorded(earlier,Snapshots.HISTORY)<Snapshots.recorded(commit,Snapshots.HISTORY));
  assertEquals(14L,s.read().format("delta").option("versionAsOf",Snapshots.recorded(commit,"bronze/events")).load(d+"/bronze/events").count());
  // The evidence digest is reproducible from the pinned assessments' source revisions.
  Dataset<Row> pinned=Snapshots.assessments(s,d,commit);
  assertEquals((long)commit.<Long>getAs("products"),pinned.count());
  Row evidence=pinned.selectExpr("explode(sourceRevisions) x").selectExpr("concat_ws(':',x.kind,x.eventId,x.revision,x.payloadHash) r")
   .agg(size(collect_set("r")).cast("long"),sha2(array_join(array_sort(collect_set("r")),"\n"),256)).first();
  assertEquals((long)commit.<Long>getAs("evidenceRevisions"),evidence.getLong(0));assertEquals(commit.<String>getAs("evidenceDigest"),evidence.getString(1));
  assertTrue(pinned.selectExpr("explode(sourceRevisions) x").filter("x.eventId = 'assign' AND x.revision = 2").count()>0,"evidence names the corrected revision");
 }
}

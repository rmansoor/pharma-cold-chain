package io.coldchain;
import java.util.*;
import java.util.regex.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
// Operating metrics against a lake built by the real pipeline, with Kafka offsets supplied by the test, and the stall injection
// the stalled-publication alert is drilled with (scripts/ops-drill.sh).
class MonitorTest {
 static SparkSession s;static String data;
 @BeforeAll static void start() {
  s=Pipeline.session("cold-chain-monitor-tests");s.sparkContext().setLogLevel("ERROR");s.conf().set(Specifications.ALLOW_SYNTHETIC,true);
  data=Pipeline.DATA;Pipeline.halt=false;PublicationTest.s=s;
 }
 @AfterAll static void stop(){Pipeline.DATA=data;Pipeline.failAt=null;Pipeline.stallAt=null;Pipeline.halt=true;s.stop();}
 // The value of one sample, or null when the scrape has none.
 static Double value(String scrape,String name,String labels) {
  Matcher m=Pattern.compile("(?m)^"+Pattern.quote(name+(labels==null?"":"{"+labels+"}"))+" (\\S+)$").matcher(scrape);
  return m.find()?Double.valueOf(m.group(1)):null;
 }
 // Every input partition the manifest covers, `ahead` records past its high-watermark; the first waiting record arrived at `arrived`.
 static List<Monitor.Partition> partitions(Row commit,long ahead,Long arrived) {
  List<Monitor.Partition> out=new ArrayList<>();
  Monitor.published(commit).forEach((k,v)->out.add(new Monitor.Partition(k.split(":")[0],Integer.parseInt(k.split(":")[1]),v+ahead,v,ahead>0?arrived:null)));
  return out;
 }
 @Test void metricsDescribeTheCommittedSnapshotAndItsBacklog()throws Exception {
  String d=PublicationTest.lake("monitor");
  PublicationTest.run(d,PublicationTest.initial(),0,PublicationTest.LINEAGE,null);
  PublicationTest.run(d,PublicationTest.correction(),1,PublicationTest.LINEAGE,null);
  Row commit=Snapshots.latest(s,d);
  long now=commit.getTimestamp(commit.fieldIndex("committedAt")).getTime()+3_600_000;
  // Idle for an hour with everything published: the commit is an hour old, yet nothing waits and the unpublished age is 0.
  String idle=Monitor.render(s,d,commit,partitions(commit,0,null),now).toString();
  assertEquals(3600.0,value(idle,"coldchain_commit_age_seconds",null),0.001);
  assertEquals(0.0,value(idle,"coldchain_backlog_records_total",null));
  assertEquals(0.0,value(idle,"coldchain_oldest_unpublished_age_seconds",null));
  // Three records waiting on every partition, the oldest for 400 s.
  List<Monitor.Partition> behind=partitions(commit,3,now-400_000);
  String busy=Monitor.render(s,d,commit,behind,now).toString();
  assertEquals(3.0*behind.size(),value(busy,"coldchain_backlog_records_total",null));
  assertEquals(400.0,value(busy,"coldchain_oldest_unpublished_age_seconds",null));
  Monitor.Partition p=behind.get(0);
  assertEquals(3.0,value(busy,"coldchain_backlog_records","topic=\""+p.topic()+"\",partition=\""+p.partition()+"\""));
  assertEquals((double)p.published(),value(busy,"coldchain_published_offset","topic=\""+p.topic()+"\",partition=\""+p.partition()+"\""));
  // Manifest figures equal what the lake holds for snapshot 1.
  Dataset<Row> state=Snapshots.assessments(s,d,commit);
  long bronze=Snapshots.table(s,d,"bronze/events").count();
  assertEquals(1.0,value(busy,"coldchain_last_snapshot_id",null));
  assertEquals(2.0,value(busy,"coldchain_commits",null));
  assertEquals((double)bronze,value(busy,"coldchain_bronze_rows",null));
  assertEquals(1.0,value(busy,"coldchain_batch_rows",null),"the correction batch holds one record");
  assertEquals((double)state.count(),value(busy,"coldchain_products",null));
  assertEquals((double)state.filter("assessment = 'QUALITY_REVIEW'").count(),value(busy,"coldchain_review_products",null));
  assertEquals((double)state.filter("assessment = 'INSUFFICIENT_DATA'").count(),value(busy,"coldchain_insufficient_products",null));
  long missing=state.filter("array_contains(dataQualityFlags,'MISSING_COVERAGE')").count();
  assertTrue(missing>0,"the corrected state has a unit with missing coverage");
  assertEquals((double)missing,value(busy,"coldchain_missing_coverage_products",null));
  long quarantined=Snapshots.table(s,d,Silver.QUARANTINE).filter("status = 'OPEN'").count(),conflicts=Snapshots.table(s,d,Silver.CASES).filter("status = 'OPEN'").count();
  assertEquals((double)quarantined,value(busy,"coldchain_quarantine_open",null));
  assertEquals((double)conflicts,value(busy,"coldchain_conflict_cases_open",null));
  assertEquals(quarantined/(double)bronze,value(busy,"coldchain_quarantine_open_ratio",null),1e-9);
  assertEquals((double)(long)commit.getAs("evidenceRevisions"),value(busy,"coldchain_evidence_revisions",null));
  assertEquals((double)(long)commit.getAs("changedEvents"),value(busy,"coldchain_changed_events",null));
  assertEquals(0.0,value(busy,"coldchain_snapshot_full","reason=\"\""),"the correction was incremental");
  for(String stage:Monitor.STAGES)assertNotNull(value(busy,"coldchain_batch_stage_seconds","stage=\""+stage+"\""),stage);
  double total=value(busy,"coldchain_batch_seconds",null),lag=value(busy,"coldchain_publication_lag_seconds",null);
  assertTrue(total>0&&lag>=0,"batch "+total+" s, lag "+lag+" s");
  // Storage per table equals the files on disk, and the Delta version is the newest commit's.
  long[] files=Monitor.size(java.nio.file.Paths.get(d,"bronze/events"));
  assertEquals((double)files[0],value(busy,"coldchain_storage_bytes","area=\"bronze/events\""));
  assertEquals((double)files[1],value(busy,"coldchain_storage_files","area=\"bronze/events\""));
  assertEquals(1.0,value(busy,"coldchain_delta_version","area=\"gold/commits\""));
  assertTrue(value(busy,"coldchain_volume_free_bytes",null)>0);
  // A manifest written before these figures existed still renders, without them.
  Row older=Snapshots.table(s,d,Snapshots.COMMITS).drop("bronzeRows","batchRows","openQuarantine","openConflicts","reviewProducts","insufficientProducts",
   "missingCoverageProducts","reviewItems","canonicalMs","silverMs","impactMs","exposureMs","publishMs","batchMs","publicationLagMs").orderBy(col("snapshotId").desc()).first();
  String legacy=Monitor.render(s,d,older,behind,now).toString();
  assertNull(value(legacy,"coldchain_batch_rows",null));
  assertEquals(400.0,value(legacy,"coldchain_oldest_unpublished_age_seconds",null));
 }
 // STALL_AT holds a batch after a stage while the process keeps running: no manifest appears, the monitor sees the records
 // wait, and once the stall is cleared the batch's retry publishes them.
 @Test void aStalledBatchPublishesNothingUntilItIsRetried()throws Exception {
  String d=PublicationTest.lake("stall");
  PublicationTest.run(d,PublicationTest.initial(),0,PublicationTest.LINEAGE,null);
  Row first=Snapshots.latest(s,d);
  Dataset<Row> correction=PublicationTest.correction();
  Pipeline.stallAt="silver:1";
  List<Throwable> ended=Collections.synchronizedList(new ArrayList<>());
  Thread batch=new Thread(()->{try{PublicationTest.run(d,correction,1,PublicationTest.LINEAGE,null);}catch(Throwable t){ended.add(t);}},"stalled-batch");
  batch.start();
  long deadline=System.currentTimeMillis()+600_000;
  while(!(batch.getState()==Thread.State.TIMED_WAITING&&Arrays.stream(batch.getStackTrace()).anyMatch(f->f.getMethodName().equals("inject"))))
   {assertTrue(batch.isAlive(),"the batch ended instead of stalling: "+ended);assertTrue(System.currentTimeMillis()<deadline,"no stall");Thread.sleep(200);}
  Thread.sleep(2000);
  assertTrue(batch.isAlive(),"the stalled batch's thread is still running");
  assertEquals(0L,(long)Snapshots.latest(s,d).getAs("snapshotId"),"no manifest while stalled");
  // The correction's record is in Bronze but no manifest covers it: one partition is one record behind.
  long now=System.currentTimeMillis();
  Map<String,Long> published=Monitor.published(Snapshots.latest(s,d));
  Row record=Snapshots.table(s,d,"bronze/events").filter("ingestBatchId = 1").first();
  String key=record.getAs("topic")+":"+record.<Integer>getAs("partition");
  List<Monitor.Partition> parts=List.of(new Monitor.Partition(record.getAs("topic"),record.getAs("partition"),record.<Long>getAs("offset")+1,
   published.getOrDefault(key,0L),record.getTimestamp(record.fieldIndex("kafkaTime")).getTime()));
  String stalled=Monitor.render(s,d,Snapshots.latest(s,d),parts,now+400_000).toString();
  assertEquals(1.0,value(stalled,"coldchain_backlog_records_total",null));
  assertTrue(value(stalled,"coldchain_oldest_unpublished_age_seconds",null)>=400,stalled);
  // Clearing the stall is a restart: the hung attempt ends and the batch is retried.
  Pipeline.stallAt=null;batch.interrupt();batch.join(60_000);
  assertFalse(batch.isAlive());
  assertTrue(ended.size()==1&&ended.get(0).getMessage().contains("stall interrupted"),String.valueOf(ended));
  PublicationTest.run(d,correction,1,PublicationTest.LINEAGE,null);
  Row retried=Snapshots.latest(s,d);
  assertEquals(1L,(long)retried.getAs("snapshotId"));
  assertNotEquals(first.<String>getAs("assessmentsDigest"),retried.<String>getAs("assessmentsDigest"));
  assertEquals(PublicationTest.latest(PublicationTest.clean()).<String>getAs("assessmentsDigest"),retried.<String>getAs("assessmentsDigest"),"the retry equals a clean run");
  List<Monitor.Partition> caught=List.of(new Monitor.Partition(parts.get(0).topic(),parts.get(0).partition(),parts.get(0).end(),Monitor.published(retried).get(key),null));
  String recovered=Monitor.render(s,d,retried,caught,now+400_000).toString();
  assertEquals(0.0,value(recovered,"coldchain_backlog_records_total",null));
  assertEquals(0.0,value(recovered,"coldchain_oldest_unpublished_age_seconds",null));
 }
}

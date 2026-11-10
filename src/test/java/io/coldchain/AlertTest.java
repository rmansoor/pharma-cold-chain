package io.coldchain;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.coldchain.IncrementalTest.revised;
import static io.coldchain.ModelTest.with;
// Review items from the real pipeline: which assessment revisions produce them, and that the relay and the case store
// deliver each one exactly once in effect through relay crashes, unpublished snapshots, consumer restarts and redelivery.
class AlertTest {
 static SparkSession s;static String data;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-alert-tests");s.sparkContext().setLogLevel("ERROR");
  s.conf().set(Specifications.ALLOW_SYNTHETIC,true);data=Pipeline.DATA;IncrementalTest.s=s;Pipeline.halt=false;}
 @AfterAll static void stop(){Pipeline.DATA=data;Pipeline.failAt=null;Pipeline.relay=null;s.stop();}
 static void publish(String dir,long id,List<Object> records)throws Exception{Pipeline.DATA=dir;Pipeline.process(IncrementalTest.batch(records),id,"lineage-alerts");}
 static List<String> items(String dir,long id) {
  return TimelineTest.strings(Snapshots.table(s,dir,Outbox.OUTBOX).filter(col("snapshotId").equalTo(id)),
   "concat(productId,' ',change,' ',changeReason,' ',coalesce(previousAssessment,'-'),'->',coalesce(assessment,'-'))");
 }
 static Dataset<Row> messages(List<Outbox.Message> sent){return s.createDataset(sent.stream().map(Outbox.Message::json).toList(),Encoders.STRING()).toDF("value");}
 static List<String> cases(String store) {
  return TimelineTest.strings(Snapshots.table(s,store,CaseStore.CASES),"concat(reviewItemId,' ',status,' ',coalesce(assessment,'-'),' ',snapshotId)");
 }
 @Test void itemsFollowEveryAssessmentRevision()throws Exception {
  String d=Files.createTempDirectory("cold-chain-alerts").toString();
  publish(d,0,new ArrayList<>(Simulator.initial()));
  assertEquals(List.of("UNIT-A NEW ENTERED_REVIEW -->QUALITY_REVIEW","UNIT-B NEW ENTERED_REVIEW -->INSUFFICIENT_DATA","UNIT-D NEW ENTERED_REVIEW -->QUALITY_REVIEW"),items(d,0));
  // The sensor was in BOX-B: every unit is still under review, with a different assessment.
  publish(d,1,List.of(revised(3,"containerId","BOX-B")));
  assertEquals(List.of("UNIT-A REVISED ASSESSMENT_CHANGED QUALITY_REVIEW->INSUFFICIENT_DATA","UNIT-B REVISED ASSESSMENT_CHANGED INSUFFICIENT_DATA->QUALITY_REVIEW",
   "UNIT-D REVISED ASSESSMENT_CHANGED QUALITY_REVIEW->INSUFFICIENT_DATA"),items(d,1));
  // A retry of the same correction recomputes nothing and notifies nobody.
  publish(d,2,List.of(revised(3,"containerId","BOX-B")));
  assertEquals(List.of(),items(d,2));
  // PALLET-1 leaves BOX-A at 12:05: UNIT-A and UNIT-D are recomputed, but only UNIT-D's assessment changes (its lineage
  // revision), so UNIT-A's case hears nothing.
  publish(d,3,List.of(revised(9,"endTime",IncrementalTest.t(5))));
  assertEquals(List.of("UNIT-A","UNIT-D"),TimelineTest.strings(Snapshots.table(s,d,Snapshots.HISTORY).filter("snapshotId = 3"),"productId"));
  assertEquals(List.of("UNIT-D REVISED ASSESSMENT_CHANGED INSUFFICIENT_DATA->INSUFFICIENT_DATA"),items(d,3));
  // The hot reading was 5 °C: UNIT-B no longer needs review.
  publish(d,4,List.of(revised(6,"temperatureC",5.0)));
  assertEquals(List.of("UNIT-B WITHDRAWN LEFT_REVIEW QUALITY_REVIEW->NO_OBSERVED_EXCURSION"),items(d,4));
  // Revision 3 restores 12 °C: UNIT-B needs review again.
  publish(d,5,List.of(revised(6,"temperatureC",12.0,"revision",3)));
  assertEquals(List.of("UNIT-B NEW ENTERED_REVIEW NO_OBSERVED_EXCURSION->QUALITY_REVIEW"),items(d,5));
  // A conflicting payload withholds UNIT-B's only placement: its case is withdrawn, keeping the evidence it last rested on.
  publish(d,6,List.of(with(2,"containerId","BOX-Z")));
  assertEquals(List.of("UNIT-B WITHDRAWN UNIT_REMOVED QUALITY_REVIEW->-"),items(d,6));
  Row removed=Snapshots.table(s,d,Outbox.OUTBOX).filter("snapshotId = 6").first();
  assertEquals(5L,(long)removed.getAs("previousSnapshotId"));assertFalse(removed.getList(removed.fieldIndex("sourceRevisions")).isEmpty());
  assertEquals(List.of("LOT-1"),removed.getList(removed.fieldIndex("lotIds")));
  // Each item names the assessment revision it reports, its reasons and evidence; delivery IDs are unique.
  Row b=Snapshots.table(s,d,Outbox.OUTBOX).filter("snapshotId = 5").first();
  assertEquals(List.of("EXCURSION_ABOVE_ALLOWANCE"),b.getList(b.fieldIndex("assessmentReasons")));
  assertTrue(b.<Row>getList(b.fieldIndex("sourceRevisions")).stream().anyMatch(r->"reading-hot".equals(r.getAs("eventId"))&&(long)r.getAs("revision")==3L));
  assertEquals(Snapshots.table(s,d,Outbox.OUTBOX).count(),Snapshots.table(s,d,Outbox.OUTBOX).select("deliveryId").distinct().count());
  // Relayed and applied, the case store holds each unit's newest item.
  List<Outbox.Message> sent=new ArrayList<>();
  assertEquals(10L,Outbox.drain(s,d,(id,m)->sent.addAll(m)));
  String store=Files.createTempDirectory("cold-chain-cases").toString();
  CaseStore.apply(CaseStore.parse(messages(sent)),store,0);
  assertEquals(List.of("UNIT-A OPEN INSUFFICIENT_DATA 1","UNIT-B WITHDRAWN - 6","UNIT-D OPEN INSUFFICIENT_DATA 3"),cases(store));
  assertEquals(0L,Outbox.drain(s,d,(id,m)->fail("nothing is left to relay")));
 }
 // The published examples (contracts/review-items) are what the relay sends: they parse into the item contract unchanged.
 @Test void examplesMatchTheItemContract()throws Exception {
  for(Path f:Files.newDirectoryStream(Paths.get("contracts/review-items"),"*.json")) {
   String json=Files.readString(f);
   Row parsed=CaseStore.parse(s.createDataset(List.of(json),Encoders.STRING()).toDF("value")).selectExpr("to_json(struct(*))").first();
   assertEquals(Simulator.JSON.readTree(json),Simulator.JSON.readTree(parsed.getString(0)),f.toString());
  }
 }
 @Test void relayAndCaseStoreSurviveCrashesWithoutLossOrDuplicates()throws Exception {
  String d=Files.createTempDirectory("cold-chain-relay").toString(),store=Files.createTempDirectory("cold-chain-cases").toString();
  List<Outbox.Message> sent=new ArrayList<>();
  Pipeline.relay=(id,m)->sent.addAll(m);
  try {
   publish(d,0,new ArrayList<>(Simulator.initial()));
   assertEquals(3,sent.size(),"the pipeline relays a snapshot's items once it is committed");
   // The relay stops after Kafka accepted snapshot 1's items and before recording them: they are sent again later.
   Pipeline.failAt="relay:1";
   publish(d,1,List.of(revised(3,"containerId","BOX-B")));
   assertEquals(6,sent.size());assertEquals(List.of(1L),Outbox.pending(s,d));
   // Snapshot 2 stops after writing its items, before its manifest. A drain meanwhile (the timer, say) resends snapshot 1
   // and leaves snapshot 2's uncommitted items alone; they are sent once the retry publishes them.
   Pipeline.failAt="outbox:2";
   assertThrows(IllegalStateException.class,()->publish(d,2,List.of(revised(6,"temperatureC",5.0))));
   assertEquals(6,sent.size(),"a failed batch relays nothing");
   assertEquals(1L,Snapshots.uncommitted(s,d,Outbox.OUTBOX).count());
   Pipeline.failAt=null;
   assertEquals(3L,Outbox.drain(s,d,Pipeline.relay),"uncommitted items are never relayed");
   assertEquals(List.of(),Outbox.pending(s,d));
   publish(d,2,List.of(revised(6,"temperatureC",5.0)));
   assertEquals(List.of(),Outbox.pending(s,d));
   assertEquals(10,sent.size(),"snapshot 1 resent once, snapshot 2 sent once its manifest exists");
  } finally {Pipeline.relay=null;Pipeline.failAt=null;}
  Dataset<Row> committed=Snapshots.table(s,d,Outbox.OUTBOX).join(Snapshots.table(s,d,Snapshots.COMMITS).select("snapshotId"),"snapshotId","left_semi");
  assertEquals(7L,committed.count());
  assertTrue(committed.select("deliveryId").except(CaseStore.parse(messages(sent)).select("deliveryId")).isEmpty(),"every committed item was relayed");
  // The consumer stops after recording notifications and before updating cases, and reprocesses that batch on restart;
  // then everything is delivered again, and finally snapshot 1's items arrive once more, after snapshot 2's.
  Pipeline.failAt="cases:0";
  assertThrows(IllegalStateException.class,()->CaseStore.apply(CaseStore.parse(messages(sent.subList(0,6))),store,0));
  Pipeline.failAt=null;
  CaseStore.apply(CaseStore.parse(messages(sent.subList(0,6))),store,0);
  CaseStore.apply(CaseStore.parse(messages(sent)),store,1);
  CaseStore.apply(CaseStore.parse(messages(sent.subList(3,6))),store,2);
  // The same result as one clean delivery of each committed item.
  String clean=Files.createTempDirectory("cold-chain-cases-clean").toString();
  CaseStore.apply(CaseStore.parse(messages(sent)).dropDuplicates("deliveryId"),clean,0);
  assertEquals(cases(clean),cases(store));
  assertEquals(List.of("UNIT-A OPEN INSUFFICIENT_DATA 1","UNIT-B WITHDRAWN NO_OBSERVED_EXCURSION 2","UNIT-D OPEN INSUFFICIENT_DATA 1"),cases(store));
  Dataset<Row> told=Snapshots.table(s,store,CaseStore.NOTIFICATIONS);
  assertEquals(7L,told.count(),"one notification per committed item");
  assertTrue(committed.select("deliveryId").except(told.select("deliveryId")).isEmpty(),"no committed item was lost");
 }
}

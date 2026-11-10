package io.coldchain;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
import io.coldchain.ImpactQueries.*;
// Investigation queries against a lake built by the real pipeline. Besides Simulator.initial() (BOX-B, holding UNIT-B, ships to
// HOSPITAL-DEMO at 12:09), UNIT-E (lot LOT-2, no inventory) is also in BOX-B, BOX-A ships to HOSPITAL-2 at 12:05 with UNIT-A and
// UNIT-D in it (UNIT-F left BOX-A at 12:04, so it stays behind), and UNIT-A alone ships to PHARMACY-1 at 12:30.
class QueryTest {
 static SparkSession s;static String lake,data;
 static final Scope QA=Scope.investigator("qa");
 @BeforeAll static void start()throws Exception {
  s=Pipeline.session("cold-chain-query-tests");s.sparkContext().setLogLevel("ERROR");s.conf().set(Specifications.ALLOW_SYNTHETIC,true);
  data=Pipeline.DATA;IncrementalTest.s=s;
  lake=Files.createTempDirectory("cold-chain-query").toString();Pipeline.DATA=lake;
  List<Object> events=new ArrayList<>(Simulator.initial());
  events.add(Simulator.event("place-E","PLACEMENT","productId","UNIT-E","productCode","DRUG-DEMO","lotId","LOT-2","containerId","BOX-B",
   "startTime",t(0),"endTime",t(10)));
  events.add(Simulator.event("place-F","PLACEMENT","productId","UNIT-F","productCode","DRUG-DEMO","lotId","LOT-1","containerId","BOX-A",
   "startTime",t(0),"endTime",t(4)));
  events.add(Simulator.event("shipment-A","SHIPMENT","shipmentId","SHP-2","recipientId","HOSPITAL-2","containerId","BOX-A","startTime",t(5),"endTime",t(50)));
  events.add(Simulator.event("shipment-unit-A","SHIPMENT","shipmentId","SHP-3","recipientId","PHARMACY-1","productId","UNIT-A","startTime",t(30),"endTime",t(55)));
  Pipeline.process(IncrementalTest.batch(events),0,"lineage-query");
 }
 @AfterAll static void stop(){Pipeline.DATA=data;s.stop();}
 static String t(int minute){return LineageTest.t(minute);}
 static Filter by(String field,String value) {
  return switch(field){
   case "product"->new Filter(value,null,null,null,null,null);case "lot"->new Filter(null,value,null,null,null,null);
   case "shipment"->new Filter(null,null,value,null,null,null);case "location"->new Filter(null,null,null,value,null,null);
   case "recipient"->new Filter(null,null,null,null,value,null);default->new Filter(null,null,null,null,null,value);};
 }
 static List<String> ids(Page p){return p.rows().stream().map(r->r.<String>getAs("productId")).toList();}
 static List<String> shipments(Row r) {
  List<String> out=new ArrayList<>();
  if(!r.isNullAt(r.fieldIndex("shipments")))for(Row x:r.<Row>getList(r.fieldIndex("shipments")))out.add(x.getAs("shipmentId")+">"+x.getAs("recipientId"));
  return out;
 }
 static Page all(Scope scope,Filter f){return ImpactQueries.query(s,lake,scope,f,100,null);}
 @Test void everyKeyFindsTheAffectedUnitsWithTheirEvidence() {
  assertEquals(List.of("UNIT-E"),ids(all(QA,by("lot","LOT-2"))));
  assertEquals(List.of("UNIT-A","UNIT-B","UNIT-D","UNIT-F"),ids(all(QA,by("lot","LOT-1"))));
  assertEquals(List.of("UNIT-B","UNIT-E"),ids(all(QA,by("shipment","SHP-DEMO"))),"a container line ships every unit in it at dispatch");
  assertEquals(List.of("UNIT-A","UNIT-D"),ids(all(QA,by("recipient","HOSPITAL-2"))),"UNIT-D travels in BOX-A through its carton and pallet; UNIT-F was no longer in it at dispatch");
  assertEquals(List.of("UNIT-B"),ids(all(QA,by("location","HOSPITAL-DEMO"))));
  assertEquals(List.of("UNIT-A","UNIT-D","UNIT-F"),ids(all(QA,by("assessment","QUALITY_REVIEW"))));
  Row a=all(QA,by("product","UNIT-A")).rows().get(0);
  assertEquals(List.of("SHP-2>HOSPITAL-2","SHP-3>PHARMACY-1"),shipments(a));
  assertEquals("QUALITY_REVIEW",a.getAs("assessment"));assertEquals(List.of("EXCURSION_ABOVE_ALLOWANCE"),a.getList(a.fieldIndex("assessmentReasons")));
  assertFalse(a.getList(a.fieldIndex("sourceRevisions")).isEmpty());assertEquals(List.of("LOT-1"),a.getList(a.fieldIndex("lotIds")));
  // Latest known location, when it was recorded, and whether it is stale, as of the snapshot.
  assertEquals("WAREHOUSE-1",a.getAs("location"));assertEquals("2026-01-01T12:00:00Z",a.<java.sql.Timestamp>getAs("inventoryRecordedAt").toInstant().toString());
  assertEquals(0L,(long)a.getAs("inventoryAgeSeconds"));assertFalse((boolean)a.getAs("inventoryStale"));
  assertEquals("2026-01-01T12:00:00Z",a.<java.sql.Timestamp>getAs("dataAsOf").toInstant().toString());
  Row e=all(QA,by("product","UNIT-E")).rows().get(0);
  assertEquals("UNKNOWN",e.getAs("location"));assertNull(e.getAs("inventoryRecordedAt"));assertTrue(e.getList(e.fieldIndex("dataQualityFlags")).contains("UNKNOWN_INVENTORY"));
  // The intervals behind the assessment.
  long snapshot=all(QA,Filter.none()).snapshotId();
  assertEquals(List.of("12:00-12:10"),TimelineTest.strings(ImpactQueries.evidence(s,lake,QA,snapshot,"UNIT-A").filter("metric = 'EXCURSION'"),
   "concat(date_format(startTime,'HH:mm'),'-',date_format(endTime,'HH:mm'))"));
 }
 @Test void scopesLimitWhatEachCallerReads() {
  // A recipient sees the units shipped to it, and only its own shipments of them.
  Page hospital=all(Scope.recipient("hospital-2","HOSPITAL-2"),Filter.none());
  assertEquals(List.of("UNIT-A","UNIT-D"),ids(hospital));
  assertEquals(List.of("SHP-2>HOSPITAL-2"),shipments(hospital.rows().get(0)),"PHARMACY-1's shipment of UNIT-A is not shown");
  assertEquals(List.of("UNIT-B","UNIT-E"),ids(all(Scope.recipient("demo","HOSPITAL-DEMO"),Filter.none())));
  // A site sees the units last known there, without shipments.
  Page site=all(Scope.site("warehouse","WAREHOUSE-1"),Filter.none());
  assertEquals(List.of("UNIT-A"),ids(site));assertEquals(List.of(),shipments(site.rows().get(0)));
  // Asking outside the scope is refused rather than answered with nothing.
  assertThrows(AccessDenied.class,()->all(Scope.recipient("hospital-2","HOSPITAL-2"),by("recipient","HOSPITAL-DEMO")));
  assertThrows(AccessDenied.class,()->all(Scope.site("warehouse","WAREHOUSE-1"),by("location","HOSPITAL-DEMO")));
  long snapshot=hospital.snapshotId();
  assertThrows(AccessDenied.class,()->ImpactQueries.evidence(s,lake,Scope.recipient("hospital-2","HOSPITAL-2"),snapshot,"UNIT-B"));
  assertFalse(ImpactQueries.evidence(s,lake,Scope.recipient("demo","HOSPITAL-DEMO"),snapshot,"UNIT-B").isEmpty());
  // Filters inside the scope narrow it further.
  assertEquals(List.of("UNIT-E"),ids(all(Scope.recipient("demo","HOSPITAL-DEMO"),by("lot","LOT-2"))));
 }
 @Test void pagesStayOnTheirSnapshotWhileNewOnesArePublished()throws Exception {
  // Separate lake: this test publishes a correction between pages.
  String own=Files.createTempDirectory("cold-chain-paging").toString(),shared=lake;
  try {
   lake=own;Pipeline.DATA=own;
   Pipeline.process(IncrementalTest.batch(new ArrayList<Object>(Simulator.initial())),0,"lineage-paging");
   Row first=Snapshots.latest(s,own);
   List<String> expected=TimelineTest.strings(Snapshots.assessments(s,own,first).filter(array_contains(col("lotIds"),"LOT-1")),"concat(productId,' ',assessmentHash)");
   Page p1=ImpactQueries.query(s,own,QA,by("lot","LOT-1"),1,null);
   assertEquals(0L,p1.snapshotId());assertNotNull(p1.next());
   // The sensor was in BOX-B: snapshot 1 changes UNIT-A, UNIT-B and UNIT-D while the reader is between pages.
   Map<String,Object> correction=ModelTest.with(3,"containerId","BOX-B");correction.put("revision",2);
   Pipeline.process(IncrementalTest.batch(List.<Object>of(correction)),1,"lineage-paging");
   List<String> seen=new ArrayList<>();Page p=p1;
   while(true) {
    assertEquals(0L,p.snapshotId(),"every page reads the pinned snapshot");
    for(Row r:p.rows())seen.add(r.getAs("productId")+" "+r.getAs("assessmentHash"));
    if(p.next()==null)break;
    p=ImpactQueries.query(s,own,QA,by("lot","LOT-1"),1,p.next());
   }
   assertEquals(expected,seen,"no unit missed, repeated or taken from the newer snapshot");
   // A new query sees the correction.
   Page fresh=ImpactQueries.query(s,own,QA,by("product","UNIT-B"),10,null);
   assertEquals(1L,fresh.snapshotId());assertEquals("QUALITY_REVIEW",fresh.rows().get(0).getAs("assessment"));
   // A cursor works only for the query and scope it came from, and only names committed snapshots.
   String next=p1.next();
   assertThrows(IllegalArgumentException.class,()->ImpactQueries.query(s,own,QA,by("lot","LOT-2"),1,next));
   assertThrows(IllegalArgumentException.class,()->ImpactQueries.query(s,own,Scope.investigator("someone-else"),by("lot","LOT-1"),1,next));
   assertThrows(IllegalArgumentException.class,()->ImpactQueries.query(s,own,QA,by("lot","LOT-1"),1,"not-a-cursor"));
   String forged=ImpactQueries.cursor(99,"UNIT-A",ImpactQueries.fingerprint(QA,by("lot","LOT-1")));
   IllegalArgumentException e=assertThrows(IllegalArgumentException.class,()->ImpactQueries.query(s,own,QA,by("lot","LOT-1"),1,forged));
   assertTrue(e.getMessage().contains("not committed"),e.getMessage());
  } finally {lake=shared;Pipeline.DATA=shared;}
 }
}

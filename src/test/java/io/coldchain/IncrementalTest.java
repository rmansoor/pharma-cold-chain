package io.coldchain;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.coldchain.ModelTest.with;
// Runs the real pipeline through a sequence of corrections of every kind and checks, after each batch, that the incremental
// snapshot recomputed exactly the affected units and that its state equals a full recomputation of the same canonical events.
class IncrementalTest {
 static SparkSession s;static String data;static long offset;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-incremental-tests");s.sparkContext().setLogLevel("ERROR");
  s.conf().set(Specifications.ALLOW_SYNTHETIC,true);data=Pipeline.DATA;}
 @AfterAll static void stop(){Pipeline.DATA=data;s.stop();}
 static String t(int minute){return LineageTest.t(minute);}
 static Dataset<Row> batch(List<Object> records)throws Exception {
  List<Row> rows=new ArrayList<>();
  for(Object r:records) {
   String json=r instanceof String raw?raw:Simulator.JSON.writeValueAsString(r);
   String kind=r instanceof Map<?,?> m?(String)m.get("kind"):"READING";
   rows.add(RowFactory.create(Simulator.topic(kind),offset++,json));
  }
  return s.createDataFrame(rows,new org.apache.spark.sql.types.StructType().add("topic","string").add("offset","long").add("value","string"))
   .selectExpr("topic","0 partition","offset","current_timestamp() timestamp","CAST(NULL AS binary) key","CAST(value AS binary) value",
    "CAST(array() AS array<struct<key:string,value:binary>>) headers");
 }
 // Publishes one batch and checks the snapshot against a full recomputation of the canonical events it used.
 static Row step(String dir,long id,String mode,List<String> recomputed,List<Object> records)throws Exception {
  Pipeline.DATA=dir;Pipeline.process(batch(records),id,"lineage-incremental");
  Row c=Snapshots.latest(s,dir);
  assertEquals(id,(long)c.getAs("snapshotId"));assertEquals(mode,c.getAs("recomputeMode"),"snapshot "+id);
  assertEquals(mode,c.getAs("silverMode"),"Silver of snapshot "+id);
  assertSilverMatchesFullBuild(s,dir,c);
  // The units the snapshot wrote rows for (a tombstone included) are exactly the affected ones.
  if(recomputed!=null)assertEquals(recomputed,TimelineTest.strings(Snapshots.table(s,dir,Snapshots.HISTORY).filter(col("snapshotId").equalTo(id)),
   "concat(productId,IF(coalesce(removed,false),' removed',''))"),"recomputed units of snapshot "+id);
  Dataset<Row> canonical=s.read().format("delta").option("versionAsOf",Snapshots.recorded(c,"silver/events_current")).load(dir+"/silver/events_current");
  Model.Result full=Model.exposure(s,canonical);
  Row reference=Snapshots.digests(full.assessments(),full.intervals()).first();
  for(String d:Replay.DIGESTS)assertEquals(reference.<Object>getAs(d),c.<Object>getAs(d),d+" of snapshot "+id+" equals a full recomputation");
  return c;
 }
 // Every Silver table the snapshot recorded equals a full build from the same Bronze version, row for row.
 static void assertSilverMatchesFullBuild(SparkSession s,String dir,Row c)throws Exception {
  String reference=Files.createTempDirectory("cold-chain-silver").toString();
  Map<String,Long> versions=new LinkedHashMap<>();
  long started=System.nanoTime();
  Silver.full(s,reference,0,s.read().format("delta").option("versionAsOf",Snapshots.recorded(c,"bronze/events")).load(dir+"/bronze/events"),versions,"REFERENCE");
  assertEquals(List.of(),Silver.differences(s,dir,Silver.versions(c),reference,versions),"Silver of snapshot "+c.getAs("snapshotId")+" differs from a full build");
  System.out.println("SILVER CHECK ms="+(System.nanoTime()-started)/1_000_000);
 }
 // A correction: the base event at the next revision with the given fields changed.
 static Map<String,Object> revised(int base,Object... fields) {
  Map<String,Object> e=with(base,fields);
  if(!Arrays.asList(fields).contains("revision"))e.put("revision",2);
  return e;
 }
 @Test void incrementalSnapshotsEqualFullRecomputation()throws Exception {
  String d=Files.createTempDirectory("cold-chain-incremental").toString();
  List<Object> initial=new ArrayList<>(Simulator.initial());
  step(d,0,"FULL",List.of("UNIT-A","UNIT-B","UNIT-D"),initial);
  // The sensor was in BOX-B: units of the container it left and of the one it names now.
  step(d,1,"INCREMENTAL",List.of("UNIT-A","UNIT-B","UNIT-D"),List.of(revised(3,"containerId","BOX-B")));
  // PALLET-1 leaves BOX-A at 12:05: everything below the pallet and below BOX-A.
  step(d,2,"INCREMENTAL",List.of("UNIT-A","UNIT-D"),List.of(revised(9,"endTime",t(5))));
  // A specification change reaches every unit of its product code.
  step(d,3,"INCREMENTAL",List.of("UNIT-A","UNIT-B","UNIT-D"),List.of(revised(0,"maxC",15.0)));
  // SENSOR-1 is now in BOX-B only; SENSOR-9 is assigned nowhere, so its reading affects nobody.
  step(d,4,"INCREMENTAL",List.of("UNIT-B"),List.of(revised(6,"temperatureC",5.0),
   Simulator.event("reading-9","READING","sensorId","SENSOR-9","startTime",t(0),"endTime",t(10),"temperatureC",30.0)));
  // Custody of BOX-A.
  step(d,5,"INCREMENTAL",List.of("UNIT-A","UNIT-D"),List.of(revised(11,"custodianId","CARRIER-2")));
  // Inventory of UNIT-A, a retry of an unchanged event and a malformed record: only the inventory changes anything.
  Row inventory=step(d,6,"INCREMENTAL",List.of("UNIT-A"),List.of(revised(4,"location","WAREHOUSE-9"),revised(6,"temperatureC",5.0),"{broken"));
  assertEquals(1L,(long)inventory.getAs("changedEvents"));
  // Readers see the same assessments, inventory age and flags included, as a full computation as of the same events.
  Dataset<Row> canonical=s.read().format("delta").option("versionAsOf",Snapshots.recorded(inventory,"silver/events_current")).load(d+"/silver/events_current");
  String row="concat(productId,' ',assessmentHash,' ',coalesce(CAST(inventoryAgeSeconds AS string),'-'),' ',array_join(dataQualityFlags,','))";
  assertEquals(TimelineTest.strings(Model.assessments(s,canonical),row),TimelineTest.strings(Snapshots.assessments(s,d,inventory),row));
  // A conflicting second payload for place-B withholds it: UNIT-B has no membership left, so it leaves the state.
  Map<String,Object> conflict=with(2,"containerId","BOX-Z");
  Row removed=step(d,7,"INCREMENTAL",List.of("UNIT-B removed"),List.of(conflict));
  assertEquals(List.of("UNIT-A","UNIT-D"),TimelineTest.strings(Snapshots.assessments(s,d,removed),"productId"));
  assertEquals(List.of("UNIT-A","UNIT-B","UNIT-D"),TimelineTest.strings(Snapshots.assessments(s,d,Snapshots.table(s,d,Snapshots.COMMITS)
   .filter("snapshotId = 6").first()),"productId"),"the previous snapshot is unchanged");
  // SENSOR-1 is also assigned to BOX-A again: the new assignment contradicts the one to BOX-B, so units of both are affected.
  step(d,8,"INCREMENTAL",List.of("UNIT-A","UNIT-D"),List.of(Simulator.event("assign-again","ASSIGNMENT","sensorId","SENSOR-1","containerId","BOX-A",
   "startTime",t(0),"endTime",t(10))));
  // The impact log names each changed event with both revisions.
  assertEquals(List.of("1 assign REVISED 1->2","7 place-B WITHHELD 1->-","8 assign-again ADDED -->1"),TimelineTest.strings(
   Snapshots.table(s,d,Snapshots.IMPACTS).filter("snapshotId IN (1,7,8)"),
   "concat(snapshotId,' ',eventId,' ',change,' ',coalesce(CAST(oldRevision AS string),'-'),'->',coalesce(CAST(newRevision AS string),'-'))"));
  // Allowing synthetic thresholds no longer: a configuration change, approved by quality (GovernanceTest refuses one that is
  // not), recomputes everything.
  s.conf().set(Specifications.ALLOW_SYNTHETIC,false);
  Pipeline.DATA=d;Governance.approve(s,"demo-quality","RULE_CHANGE",Governance.ruleSubject(false),null,"stop using synthetic thresholds");
  try {
   Row config=step(d,9,"FULL",List.of("UNIT-A","UNIT-D"),List.of(revised(10,"custodianId","CARRIER-3")));
   assertEquals("CONFIGURATION_CHANGED",config.getAs("recomputeReason"));
  } finally {s.conf().set(Specifications.ALLOW_SYNTHETIC,true);}
 }
 // Corrections aimed at every Silver table. step checks each snapshot's Silver against a full build, and its Gold against a
 // full recomputation.
 @Test void incrementalSilverEqualsAFullBuild()throws Exception {
  String d=Files.createTempDirectory("cold-chain-incremental-silver").toString();
  step(d,0,"FULL",null,new ArrayList<>(Simulator.initial()));
  // Calibration ends at 12:05: SENSOR-1's reading is only partly calibrated.
  step(d,1,"INCREMENTAL",null,List.of(revised(12,"endTime",t(5))));
  // The shipment carried BOX-A, not BOX-B.
  step(d,2,"INCREMENTAL",null,List.of(revised(13,"containerId","BOX-A")));
  // CARTON-1 moves to a new PALLET-2 in BOX-B: UNIT-D's container path changes, and with it shipments and invalidations.
  step(d,3,"INCREMENTAL",null,List.of(revised(8,"parentId","PALLET-2"),
   Simulator.event("pack-pallet-2","PACKING","childId","PALLET-2","childLevel","PALLET","parentId","BOX-B","parentLevel","CONTAINER","startTime",t(0),"endTime",t(10))));
  // The sensor was in BOX-B (an assignment correction, so invalidations list the units of both boxes); then UNIT-D leaves its
  // carton at 12:08: a unit-level change whose container is three memberships up, which changes UNIT-D's path in BOX-B and
  // with it the invalidation rows of the corrected assignment.
  step(d,4,"INCREMENTAL",null,List.of(revised(3,"containerId","BOX-B")));
  step(d,5,"INCREMENTAL",null,List.of(revised(7,"endTime",t(8))));
  // CARTON-1 is also used as a pallet in BOX-A (LEVEL_MISMATCH, CONTRADICTORY_PARENT), and BOX-B sits on PALLET-2 (CYCLE).
  step(d,6,"INCREMENTAL",null,List.of(Simulator.event("pack-odd","PACKING","childId","CARTON-1","childLevel","PALLET","parentId","BOX-A","parentLevel","CONTAINER",
   "startTime",t(0),"endTime",t(10))));
  step(d,7,"INCREMENTAL",null,List.of(Simulator.event("pack-loop","PACKING","childId","BOX-B","childLevel","PALLET","parentId","PALLET-2","parentLevel","CONTAINER",
   "startTime",t(0),"endTime",t(10))));
  // The loop is corrected away; a second custodian overlaps BOX-A's.
  Map<String,Object> unlooped=Simulator.event("pack-loop","PACKING","childId","PALLET-9","childLevel","PALLET","parentId","BOX-C","parentLevel","CONTAINER",
   "startTime",t(0),"endTime",t(10));
  unlooped.put("revision",2);
  step(d,8,"INCREMENTAL",null,List.of(unlooped,Simulator.event("custody-A-3","CUSTODY","containerId","BOX-A","custodianId","CARRIER-9","startTime",t(0),"endTime",t(10))));
  // An out-of-range reading is quarantined, then resolved by a valid higher revision; records without an eventId stay open.
  Map<String,Object> bad=Simulator.event("reading-bad","READING","sensorId","SENSOR-1","startTime",t(0),"endTime",t(10),"temperatureC",500.0);
  step(d,9,"INCREMENTAL",null,List.of(bad,"{broken","{\"kind\":\"READING\"}"));
  Map<String,Object> fixed=new LinkedHashMap<>(bad);fixed.put("revision",2);fixed.put("temperatureC",6.0);
  step(d,10,"INCREMENTAL",null,List.of(fixed));
  // A second payload for reading-hot revision 1 withholds it; revision 2 resolves the case.
  step(d,11,"INCREMENTAL",null,List.of(with(6,"temperatureC",3.0)));
  step(d,12,"INCREMENTAL",null,List.of(revised(6,"temperatureC",4.0)));
  // The assignment now ends at 12:07: contradictions, timelines, invalidations and sampling gaps of the assignment.
  step(d,13,"INCREMENTAL",null,List.of(with(3,"revision",3,"containerId","BOX-B","endTime",t(7))));
  // Both of SENSOR-1's readings now end early (12:06 and 12:03): the sampling gaps of its assignment change.
  Map<String,Object> shorter=new LinkedHashMap<>(fixed);shorter.put("revision",3);shorter.put("endTime",t(3));
  Row last=step(d,14,"INCREMENTAL",null,List.of(revised(6,"revision",3,"temperatureC",4.0,"endTime",t(6)),shorter));
  assertTrue(Snapshots.table(s,d,Silver.LINEAGE_ANOMALIES).filter("anomaly = 'LEVEL_MISMATCH'").count()>0,"the scenario reaches lineage anomalies");
  assertTrue(Snapshots.table(s,d,Silver.QUARANTINE).filter("status = 'RESOLVED'").count()>0,"and a resolved quarantine record");
  assertEquals(14L,Snapshots.table(s,d,Snapshots.COMMITS).filter("silverMode = 'INCREMENTAL'").count());
  assertNull(last.getAs("silverReason"));
 }
 @Test void replayRebuildsTheSameStateInItsOwnNamespace()throws Exception {
  String d=Files.createTempDirectory("cold-chain-live").toString();
  step(d,0,"FULL",List.of("UNIT-A","UNIT-B","UNIT-D"),new ArrayList<>(Simulator.initial()));
  Row live=step(d,1,"INCREMENTAL",List.of("UNIT-A","UNIT-B","UNIT-D"),List.of(revised(3,"containerId","BOX-B")));
  String target=Files.createTempDirectory("cold-chain-replay").toString()+"/data";
  Row replayed=Replay.run(s,d,target,null);
  assertEquals("FULL",replayed.getAs("recomputeMode"));assertEquals("REPLAY",replayed.getAs("recomputeReason"));
  assertTrue(replayed.<String>getAs("lineageId").startsWith("replay-"));
  for(String digest:Replay.DIGESTS)assertEquals(live.<Object>getAs(digest),replayed.<Object>getAs(digest),digest);
  assertEquals((long)live.getAs("snapshotId"),(long)Replay.counterpart(s,d,Snapshots.recorded(live,"bronze/events")).getAs("snapshotId"));
  // A replay never writes into the namespace it reads, nor into one already used.
  assertThrows(IllegalArgumentException.class,()->Replay.run(s,d,d,null));
  assertThrows(IllegalStateException.class,()->Replay.run(s,d,target,null));
  assertEquals(2L,Snapshots.table(s,d,Snapshots.COMMITS).count(),"the source is only read");
 }
}

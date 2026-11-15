package io.coldchain;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.junit.jupiter.api.Assertions.*;
// The run-scoped scenario through the real pipeline, in-process: each phase is acknowledged only by a snapshot covering it and
// meets its expectations there; a second run in the same lake passes on its own records without changing the first; and a run
// that sent nothing, or a later phase not sent yet, is never acknowledged by what the lake already holds.
class ScenarioTest {
 static SparkSession s;static String data;static long offset;
 @BeforeAll static void start() {
  s=Pipeline.session("cold-chain-scenario-tests");s.sparkContext().setLogLevel("ERROR");s.conf().set(Specifications.ALLOW_SYNTHETIC,true);
  data=Pipeline.DATA;Pipeline.halt=false;
 }
 @AfterAll static void stop(){Pipeline.DATA=data;Pipeline.halt=true;s.stop();}
 // One Kafka micro-batch of the phase's records, on partition 0 of their topics.
 static Dataset<Row> batch(String run,int phase)throws Exception {
  List<Row> rows=new ArrayList<>();
  for(String[] r:Scenario.records(run,phase))rows.add(RowFactory.create(r[0],offset++,r[2]));
  return s.createDataFrame(rows,new org.apache.spark.sql.types.StructType().add("topic","string").add("offset","long").add("value","string"))
   .selectExpr("topic","0 partition","offset","current_timestamp() timestamp","CAST(NULL AS binary) key","CAST(value AS binary) value",
    "CAST(array() AS array<struct<key:string,value:binary>>) headers");
 }
 static void assertPasses(String d,String run,int phase)throws Exception {
  Scenario.Result r=Scenario.check(s,d,run,phase);
  assertTrue(r.acknowledged(),"phase "+phase+" of "+run+" not acknowledged: "+r.problems());
  assertEquals(List.of(),r.problems(),"phase "+phase+" of "+run);
 }
 @Test void eachPhaseIsAcknowledgedByItsSnapshotAndRunsStayApart()throws Exception {
  String d=PublicationTest.lake("scenario");Pipeline.DATA=d;
  assertFalse(Scenario.check(s,d,"r1",1).acknowledged(),"an empty lake acknowledges nothing");
  Pipeline.process(batch("r1",1),0,"lineage-scenario");
  assertPasses(d,"r1",1);
  // Phase 2 is not sent yet: the lake must not acknowledge it, though it holds every event of phase 1.
  Scenario.Result early=Scenario.check(s,d,"r1",2);
  assertFalse(early.acknowledged());
  assertTrue(early.problems().contains("assign-1-r1@2"),early.problems().toString());
  Pipeline.process(batch("r1",2),1,"lineage-scenario");
  assertPasses(d,"r1",2);
  // The check compares every field: each expectation changed in one field alone is reported against the same snapshot.
  for(int i=0;i<4;i++)for(int f=0;f<3;f++) {
   List<Scenario.Expect> wrong=new ArrayList<>(Scenario.expected(2));Scenario.Expect x=wrong.get(i);
   wrong.set(i,new Scenario.Expect(x.unit(),f==0?(x.assessment().equals("QUALITY_REVIEW")?"INSUFFICIENT_DATA":"QUALITY_REVIEW"):x.assessment(),
    f==1?x.excursionSeconds()+1:x.excursionSeconds(),f==2?!x.missingCoverage():x.missingCoverage()));
   Scenario.Result r=Scenario.check(s,d,"r1",2,wrong);
   assertTrue(r.acknowledged()&&r.problems().size()==1&&r.problems().get(0).startsWith(x.unit()+": expected"),x+" field "+f+": "+r.problems());
  }
  assertFalse(Scenario.check(s,d,"r1",2,Scenario.expected(2).subList(0,3)).problems().isEmpty(),"an unexpected unit of the run is reported");
  // A second run in the same lake: acknowledged and correct on its own records; the first run's result is unchanged.
  Pipeline.process(batch("r2",1),2,"lineage-scenario");
  assertPasses(d,"r2",1);
  assertPasses(d,"r1",2);
  // A run that sent nothing cannot be satisfied by the retained results of the others.
  Scenario.Result stale=Scenario.check(s,d,"r3",1);
  assertFalse(stale.acknowledged());
  assertTrue(stale.problems().contains("quarantined {broken-r3"),stale.problems().toString());
 }
}

package io.coldchain;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.coldchain.ModelTest.with;
class TimelineTest {
 static SparkSession s;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-timeline-tests");s.sparkContext().setLogLevel("ERROR");
  // Simulator.initial() carries the demo's synthetic thresholds.
  s.conf().set(Specifications.ALLOW_SYNTHETIC,true);}
 @AfterAll static void stop(){s.stop();}
 // Indexes into Simulator.initial(): 3 assignment of SENSOR-1 to BOX-A, 6 hot reading, 10 and 11 BOX-A custody handover at 12:05.
 static Dataset<Row> valid(List<Map<String,Object>> input)throws Exception {
  List<String> json=new ArrayList<>();for(var e:input)json.add(Simulator.JSON.writeValueAsString(e));
  Dataset<Row> parsed=ModelTest.parseRaw(json,expr(ModelTest.TOPIC));
  assertEquals(0,parsed.filter("NOT valid").count(),"test events must satisfy the contract");
  return parsed.filter("valid");
 }
 static List<Map<String,Object>> initialPlus(Map<String,Object>... extra) {
  List<Map<String,Object>> input=new ArrayList<>(Simulator.initial());input.addAll(List.of(extra));return input;
 }
 static Row product(Dataset<Row> gold,String id){return gold.filter(col("productId").equalTo(id)).first();}
 static List<String> strings(Dataset<Row> rows,String expression) {
  return rows.selectExpr(expression+" v").orderBy("v").as(Encoders.STRING()).collectAsList();
 }
 static Dataset<Row> units(Dataset<Row> events){Dataset<Row> e=Lineage.edges(events);return Lineage.unitContainers(e,Lineage.anomalies(e));}
 @Test void correctionInvalidatesFormerAndRevisedMappingsWhateverTheArrivalOrder()throws Exception {
  // Revision 2 (sensor was in BOX-B) arrives before revision 1, revision 1 is retried, and revision 3 only restates the time.
  List<Map<String,Object>> input=new ArrayList<>();
  input.add(with(3,"revision",2,"containerId","BOX-B"));input.addAll(Simulator.initial());input.add(Simulator.initial().get(3));
  input.add(with(3,"revision",3,"containerId","BOX-B","eventTime","2026-01-01T12:01:00Z"));
  Dataset<Row> valid=valid(input),events=Model.canonical(valid);
  Dataset<Row> history=Timelines.history(valid,Timelines.anomalies(events));
  assertEquals(List.of("1:false:BOX-A","2:false:BOX-B","3:true:BOX-B"),
   strings(history.filter("eventId = 'assign'"),"concat(revision,':',current,':',containerId)"),"each revision once; the highest is current");
  Row first=history.filter("eventId = 'assign' AND revision = 1").first();
  assertEquals("test",first.getAs("sourceSystem"));assertEquals("coldchain.traceability",first.getAs("topic"));assertNotNull(first.getAs("offset"));
  Dataset<Row> invalid=Timelines.invalidations(history,units(events));
  assertEquals(List.of("FORMER:UNIT-A:1>2","FORMER:UNIT-D:1>2","REVISED:UNIT-B:1>2"),
   strings(invalid,"concat(side,':',productId,':',fromRevision,'>',toRevision)"),"revision 3 changes no mapping");
  Row former=invalid.filter("side = 'FORMER' AND productId = 'UNIT-A'").first();
  assertEquals("SENSOR-1",former.getAs("sensorId"));assertEquals("BOX-A",former.getAs("containerId"));
  // The late revision is what Gold uses: exposure moved to UNIT-B.
  Dataset<Row> gold=Model.assessments(s,events);
  assertEquals("QUALITY_REVIEW",product(gold,"UNIT-B").getAs("assessment"));
  assertEquals("INSUFFICIENT_DATA",product(gold,"UNIT-A").getAs("assessment"));
  assertEquals(List.of("assign"),product(gold,"UNIT-B").getList(product(gold,"UNIT-B").fieldIndex("assignmentIds")));
 }
 @Test void oneSensorInTwoContainersIsAmbiguousButTwoSensorsInOneContainerAreNot()throws Exception {
  // SENSOR-1 is also assigned to BOX-C from 12:05: which container did it measure?
  Dataset<Row> events=Model.canonical(valid(initialPlus(Simulator.event("assign-c","ASSIGNMENT","sensorId","SENSOR-1","containerId","BOX-C",
   "startTime","2026-01-01T12:05:00Z","endTime","2026-01-01T12:15:00Z"))));
  Dataset<Row> anomalies=Timelines.anomalies(events);
  assertEquals(List.of("SENSOR_IN_TWO_CONTAINERS SENSOR-1 assign,assign-c 12:05-12:10"),strings(anomalies,
   "concat(anomaly,' ',subjectId,' ',array_join(eventIds,','),' ',date_format(startTime,'HH:mm'),'-',date_format(endTime,'HH:mm'))"));
  Row a=product(Model.assessments(s,events),"UNIT-A");
  assertEquals("INSUFFICIENT_DATA",a.getAs("assessment"));assertEquals(0L,(long)a.getAs("coveredSeconds"),"neither assignment is used");
  Row h=Timelines.history(valid(initialPlus(Simulator.event("assign-c","ASSIGNMENT","sensorId","SENSOR-1","containerId","BOX-C",
   "startTime","2026-01-01T12:05:00Z","endTime","2026-01-01T12:15:00Z"))),anomalies).filter("eventId = 'assign'").first();
  assertEquals(List.of("SENSOR_IN_TWO_CONTAINERS"),h.getList(h.fieldIndex("anomalies")));
  // A second, cool sensor in BOX-A is redundant evidence: both count, and the hot one still triggers review.
  Dataset<Row> redundant=Model.canonical(valid(initialPlus(
   Simulator.event("assign-2","ASSIGNMENT","sensorId","SENSOR-2","containerId","BOX-A","startTime","2026-01-01T12:00:00Z","endTime","2026-01-01T12:10:00Z"),
   Simulator.event("reading-cool","READING","sensorId","SENSOR-2","startTime","2026-01-01T12:00:00Z","endTime","2026-01-01T12:10:00Z","temperatureC",5.0))));
  assertEquals(0,Timelines.anomalies(redundant).count());
  Row r=product(Model.assessments(s,redundant),"UNIT-A");
  assertEquals("QUALITY_REVIEW",r.getAs("assessment"));assertEquals(600L,(long)r.getAs("coveredSeconds"));assertEquals(600L,(long)r.getAs("excursionSeconds"));
 }
 @Test void custodyHandoverGapAndOverlap()throws Exception {
  // Equal-time handover at 12:05 from CARRIER-1 to WAREHOUSE-1: no gap, no overlap.
  Dataset<Row> gold=Model.assessments(s,Model.canonical(valid(Simulator.initial())));
  for(String unit:List.of("UNIT-A","UNIT-D")) {
   Row u=product(gold,unit);assertEquals(0L,(long)u.getAs("missingCustodySeconds"),unit);
   assertEquals(List.of("CARRIER-1","WAREHOUSE-1"),u.getList(u.fieldIndex("custodianIds")),unit);
  }
  Row b=product(gold,"UNIT-B");assertEquals(600L,(long)b.getAs("missingCustodySeconds"),"BOX-B has no custody");assertNull(b.getAs("custodianIds"));
  // A revision moves the second custody to 12:07: two minutes without a custodian. Exposure is unaffected.
  Row gap=product(Model.assessments(s,Model.canonical(valid(initialPlus(with(11,"revision",2,"startTime","2026-01-01T12:07:00Z"))))),"UNIT-A");
  assertEquals(120L,(long)gap.getAs("missingCustodySeconds"));assertEquals("QUALITY_REVIEW",gap.getAs("assessment"));
  // A second custodian from 12:08 contradicts WAREHOUSE-1; both are left out, so 12:05-12:10 has no known custodian.
  Dataset<Row> overlap=Model.canonical(valid(initialPlus(Simulator.event("custody-A-3","CUSTODY","containerId","BOX-A","custodianId","CARRIER-2",
   "startTime","2026-01-01T12:08:00Z","endTime","2026-01-01T12:10:00Z"))));
  assertEquals(List.of("CUSTODY_OVERLAP BOX-A custody-A-2,custody-A-3"),strings(Timelines.anomalies(overlap),"concat(anomaly,' ',subjectId,' ',array_join(eventIds,','))"));
  Row o=product(Model.assessments(s,overlap),"UNIT-A");
  assertEquals(300L,(long)o.getAs("missingCustodySeconds"));assertEquals(List.of("CARRIER-1"),o.getList(o.fieldIndex("custodianIds")));
  // The same custodian recorded twice is not a contradiction.
  Dataset<Row> repeated=Model.canonical(valid(initialPlus(Simulator.event("custody-A-4","CUSTODY","containerId","BOX-A","custodianId","WAREHOUSE-1",
   "startTime","2026-01-01T12:06:00Z","endTime","2026-01-01T12:10:00Z"))));
  assertEquals(0,Timelines.anomalies(repeated).count());
  assertEquals(0L,(long)product(Model.assessments(s,repeated),"UNIT-A").getAs("missingCustodySeconds"));
 }
}

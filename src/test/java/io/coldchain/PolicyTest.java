package io.coldchain;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.coldchain.ModelTest.with;
import static io.coldchain.TimelineTest.*;
class PolicyTest {
 static SparkSession s;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-policy-tests");s.sparkContext().setLogLevel("ERROR");
  s.conf().set(Specifications.ALLOW_SYNTHETIC,true);}
 @AfterAll static void stop(){s.stop();}
 // Indexes into Simulator.initial(): 0 spec (2-8 °C, 60 s allowance), 1 UNIT-A in BOX-A, 3 SENSOR-1 assigned to BOX-A,
 // 4 inventory-A, 6 hot reading (12 °C), 12 calibration of SENSOR-1 (±0.5 °C); all 12:00-12:10 unless stated.
 static String t(int minute){return LineageTest.t(minute);}
 static Dataset<Row> gold(Map<String,Object>... extra)throws Exception{return Model.assessments(s,Model.canonical(valid(initialPlus(extra))));}
 static Row unit(Dataset<Row> gold,String id){return product(gold,id);}
 static Row unitA(Map<String,Object>... extra)throws Exception{return unit(gold(extra),"UNIT-A");}
 static List<String> list(Row r,String field){return r.getList(r.fieldIndex(field));}
 static Map<String,Object> spec2(double maxC,long allowance) {
  return Simulator.event("spec-2","SPEC","productCode","DRUG-DEMO","startTime",t(5),"endTime",t(10),"minC",2.0,"maxC",maxC,
   "maxExcursionSeconds",allowance,"synthetic",true);
 }
 static Map<String,Object> newest(String eventTime) {
  Map<String,Object> e=Simulator.event("inventory-E","INVENTORY","productId","UNIT-E","location","WAREHOUSE-2","status","IN_STOCK");
  e.put("eventTime",eventTime);return e;
 }
 @Test void exactThresholds()throws Exception {
  // 60 s out of range against a 60 s allowance is within it; 61 s is above it.
  Row at=unitA(with(6,"revision",2,"endTime",t(1)),with(6,"eventId","reading-cool","startTime",t(1),"temperatureC",5.0));
  assertEquals(60L,(long)at.getAs("excursionSeconds"));assertEquals("NO_OBSERVED_EXCURSION",at.getAs("assessment"));assertEquals(List.of(),list(at,"assessmentReasons"));
  Row over=unitA(with(6,"revision",2,"endTime","2026-01-01T12:01:01Z"),with(6,"eventId","reading-cool","startTime","2026-01-01T12:01:01Z","temperatureC",5.0));
  assertEquals(61L,(long)over.getAs("excursionSeconds"));assertEquals("QUALITY_REVIEW",over.getAs("assessment"));
  // 8.0 °C is the limit itself, so in range; but it is within the ±0.5 °C uncertainty for 600 s.
  Row limit=unitA(with(6,"revision",2,"temperatureC",8.0));
  assertEquals(0L,(long)limit.getAs("excursionSeconds"));assertEquals("INSUFFICIENT_DATA",limit.getAs("assessment"));
  assertEquals(List.of("WITHIN_MEASUREMENT_UNCERTAINTY"),list(limit,"assessmentReasons"));
  // 7.5 °C is exactly the uncertainty below the limit: not a possible excursion.
  Row margin=unitA(with(6,"revision",2,"temperatureC",7.5));
  assertEquals(0L,(long)margin.getAs("possibleExcursionSeconds"));assertEquals("NO_OBSERVED_EXCURSION",margin.getAs("assessment"));
  // Age is measured back from the newest source event, here an inventory record for another unit a day later. inventory-A
  // (12:00) exactly the maximum age is current; one second older is stale.
  Row day=unitA(newest("2026-01-02T12:00:00Z"));
  assertEquals(86400L,(long)day.getAs("inventoryAgeSeconds"));assertFalse(list(day,"dataQualityFlags").contains("STALE_INVENTORY"));
  Row older=unitA(newest("2026-01-02T12:00:01Z"));
  assertTrue(list(older,"dataQualityFlags").contains("STALE_INVENTORY"));assertEquals("QUALITY_REVIEW",older.getAs("assessment"),"a flag never changes the outcome");
 }
 @Test void partialCoverageIsReportedWhateverTheOutcome()throws Exception {
  // Readings stop at 12:05. In range, that leaves too little evidence; out of range, review still comes first.
  Row cool=unitA(with(6,"revision",2,"endTime",t(5),"temperatureC",5.0));
  assertEquals("INSUFFICIENT_DATA",cool.getAs("assessment"));assertEquals(300L,(long)cool.getAs("missingSeconds"));
  assertEquals(List.of("MISSING_COVERAGE"),list(cool,"assessmentReasons"));assertEquals(List.of("MISSING_COVERAGE"),list(cool,"dataQualityFlags"));
  Row hot=unitA(with(6,"revision",2,"endTime",t(5)));
  assertEquals("QUALITY_REVIEW",hot.getAs("assessment"));
  assertEquals(List.of("EXCURSION_ABOVE_ALLOWANCE","MISSING_COVERAGE"),list(hot,"assessmentReasons"));
  assertEquals(List.of("MISSING_COVERAGE"),list(hot,"dataQualityFlags"));
 }
 static List<String> bySpecification(Row r) {
  List<String> out=new ArrayList<>();
  for(Row x:r.<Row>getList(r.fieldIndex("excursionBySpecification")))
   out.add(x.getAs("specificationId")+" "+x.getAs("excursionSeconds")+"/"+x.getAs("allowedExcursionSeconds"));
  return out;
 }
 @Test void mixedSpecificationPeriodsUseTheSmallestAllowance()throws Exception {
  // From 12:05, spec-2 allows up to 15 °C and 600 s. At 12 °C only the first five minutes are an excursion.
  Row hot=unitA(with(0,"revision",2,"endTime",t(5)),spec2(15.0,600));
  assertEquals(List.of("spec 300/60","spec-2 0/600"),bySpecification(hot));
  assertEquals("QUALITY_REVIEW",hot.getAs("assessment"));assertEquals(List.of("MIXED_SPECIFICATIONS"),list(hot,"dataQualityFlags"));
  // At 16 °C after 12:05 only: 300 s is within spec-2's own 600 s, but the cumulative total is held to the smallest
  // allowance that applied (spec's 60 s), so the unit is still reviewed. The breakdown shows why.
  Row late=unitA(with(0,"revision",2,"endTime",t(5)),spec2(15.0,600),with(6,"revision",2,"endTime",t(5),"temperatureC",5.0),
   with(6,"eventId","reading-late","startTime",t(5),"temperatureC",16.0));
  assertEquals(List.of("spec 0/60","spec-2 300/600"),bySpecification(late));
  assertEquals(60L,(long)late.getAs("allowedExcursionSeconds"));assertEquals("QUALITY_REVIEW",late.getAs("assessment"));
 }
 @Test void ambiguitiesAreFlaggedBesideTheOutcome()throws Exception {
  // spec-3 is approved at the same instant as spec and overlaps 12:05-12:10; CARRIER-2 claims BOX-A for the whole interval;
  // CARTON-1 is also on PALLET-2.
  Dataset<Row> g=gold(MeasurementTest.spec("spec-3","2026-01-01T12:00:00Z",5,10,20.0),
   Simulator.event("custody-A-3","CUSTODY","containerId","BOX-A","custodianId","CARRIER-2","startTime",t(0),"endTime",t(10)),
   with(8,"eventId","pack-carton-1b","parentId","PALLET-2"));
  Row a=unit(g,"UNIT-A");
  assertEquals("QUALITY_REVIEW",a.getAs("assessment"));
  assertEquals(List.of("MISSING_COVERAGE","AMBIGUOUS_CUSTODY","AMBIGUOUS_SPECIFICATION","MISSING_CUSTODY"),list(a,"dataQualityFlags"));
  // UNIT-D's carton has two parents, so it is never located: the custody and specification of BOX-A do not reach it.
  Row d=unit(g,"UNIT-D");
  assertEquals(List.of("MISSING_COVERAGE","AMBIGUOUS_LINEAGE","UNKNOWN_INVENTORY"),list(d,"dataQualityFlags"));
  assertEquals("UNKNOWN",d.getAs("location"));
  Row b=unit(g,"UNIT-B");
  assertEquals(List.of("MISSING_COVERAGE","AMBIGUOUS_SPECIFICATION","MISSING_CUSTODY"),list(b,"dataQualityFlags"));
  // SENSOR-1 is also assigned to BOX-B: both containers' units are flagged, and neither is measured.
  Dataset<Row> two=gold(with(3,"eventId","assign-B","containerId","BOX-B"));
  for(String id:List.of("UNIT-A","UNIT-B")) {
   Row r=unit(two,id);
   assertTrue(list(r,"dataQualityFlags").contains("AMBIGUOUS_ASSIGNMENT"),id);assertEquals("INSUFFICIENT_DATA",r.getAs("assessment"),id);
  }
 }
 @Test void inventoryIsStaleWhenTheUnitMovedAfterIt()throws Exception {
  // UNIT-A goes into BOX-C at 12:10, after inventory-A was recorded at 12:00.
  Row moved=unitA(with(1,"eventId","place-A2","containerId","BOX-C","startTime",t(10),"endTime",t(20)));
  assertEquals(0L,(long)moved.getAs("inventoryAgeSeconds"));assertTrue((boolean)moved.getAs("movedSinceInventory"));
  assertTrue(list(moved,"dataQualityFlags").contains("STALE_INVENTORY"));assertEquals("WAREHOUSE-1",moved.getAs("location"));
 }
 @Test void everyAssessmentNamesAPolicyThatReleasesNothing()throws Exception {
  Row policy=Policy.table(s,Model.RULE_VERSION).first();
  assertEquals(Policy.VERSION,policy.getAs("policyVersion"));assertEquals("exposure-v4",policy.getAs("ruleVersion"));
  Map<String,String> states=new HashMap<>();
  for(Row x:policy.<Row>getList(policy.fieldIndex("states"))) {
   assertFalse((boolean)x.getAs("impliesRelease"),x.<String>getAs("state"));states.put(x.getAs("state"),x.getAs("meaning"));
  }
  assertEquals(Set.of("QUALITY_REVIEW","INSUFFICIENT_DATA","NO_OBSERVED_EXCURSION"),states.keySet());
  Map<String,String> reasons=new HashMap<>();
  for(Row x:policy.<Row>getList(policy.fieldIndex("reasons")))reasons.put(x.getAs("code"),x.getAs("state"));
  // Each unit's outcome is the one its reasons map to under the persisted policy.
  for(Row r:gold().collectAsList()) {
   List<String> rs=list(r,"assessmentReasons");
   String expected=rs.stream().anyMatch(x->reasons.get(x).equals("QUALITY_REVIEW"))?"QUALITY_REVIEW":rs.isEmpty()?"NO_OBSERVED_EXCURSION":"INSUFFICIENT_DATA";
   assertEquals(expected,r.getAs("assessment"),r.<String>getAs("productId"));
   assertEquals(Policy.VERSION,r.getAs("policyVersion"));assertEquals("PRODUCT_RETAINED_HISTORY",r.getAs("assessmentScope"));
  }
  Row a=unitA();
  java.text.SimpleDateFormat hm=new java.text.SimpleDateFormat("HH:mm");hm.setTimeZone(TimeZone.getTimeZone("UTC"));
  assertEquals("12:00-12:10",hm.format(a.<java.sql.Timestamp>getAs("scopeStart"))+"-"+hm.format(a.<java.sql.Timestamp>getAs("scopeEnd")));
 }
}

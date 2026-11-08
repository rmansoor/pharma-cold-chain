package io.coldchain;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.coldchain.ModelTest.with;
import static io.coldchain.TimelineTest.*;
class MeasurementTest {
 static SparkSession s;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-measurement-tests");s.sparkContext().setLogLevel("ERROR");
  s.conf().set(Specifications.ALLOW_SYNTHETIC,true);}
 @AfterAll static void stop(){s.stop();}
 // Indexes into Simulator.initial(): 0 synthetic spec (2-8 °C, 60 s allowance, 12:00-12:10), 6 hot reading (12 °C, 12:00-12:10),
 // 12 calibration of SENSOR-1 (±0.5 °C). UNIT-A is in BOX-A, measured by SENSOR-1, from 12:00 to 12:10.
 static String t(int minute){return LineageTest.t(minute);}
 static final String SPAN="concat(date_format(startTime,'HH:mm'),'-',date_format(endTime,'HH:mm'))";
 static Row unitA(Dataset<Row> events){return product(Model.assessments(s,events),"UNIT-A");}
 static Row unitA(List<Map<String,Object>> input)throws Exception{return unitA(Model.canonical(valid(input)));}
 static List<String> reasons(Row r){return r.getList(r.fieldIndex("assessmentReasons"));}
 static Map<String,Object> spec(String id,String approved,int from,int to,double maxC) {
  Map<String,Object> e=Simulator.event(id,"SPEC","productCode","DRUG-DEMO","startTime",t(from),"endTime",t(to),"minC",2.0,"maxC",maxC,
   "maxExcursionSeconds",60,"synthetic",true);
  e.put("eventTime",approved);return e;
 }
 @Test void laterApprovalAppliesInsideAnOverlapAndATieIsReported()throws Exception {
  // spec-2, approved later, allows up to 15 °C from 12:05: the 12 °C reading is out of range only before 12:05.
  Dataset<Row> events=Model.canonical(valid(initialPlus(spec("spec-2","2026-01-01T12:30:00Z",5,15,15.0))));
  assertEquals(List.of("spec 12:00-12:05","spec-2 12:05-12:15"),strings(Specifications.effective(events),"concat(specificationId,' ',"+SPAN+")"));
  assertEquals(List.of("spec,spec-2 spec-2 LATEST_APPROVAL_APPLIES 12:05-12:10"),strings(Specifications.overlaps(events),
   "concat(array_join(specificationIds,','),' ',specificationId,' ',resolution,' ',"+SPAN+")"));
  Row a=unitA(events);
  assertEquals(300L,(long)a.getAs("excursionSeconds"));assertEquals(0L,(long)a.getAs("unspecifiedSeconds"));
  assertEquals(List.of("spec","spec-2"),a.getList(a.fieldIndex("specificationIds")));assertEquals("QUALITY_REVIEW",a.getAs("assessment"));
  // spec-3 is approved at the same instant as spec-2: 12:05-12:10 cannot be ordered, so it has no specification.
  Dataset<Row> tied=Model.canonical(valid(initialPlus(spec("spec-2","2026-01-01T12:30:00Z",5,15,15.0),spec("spec-3","2026-01-01T12:30:00Z",5,10,20.0))));
  assertEquals(List.of("spec 12:00-12:05","spec-2 12:10-12:15"),strings(Specifications.effective(tied),"concat(specificationId,' ',"+SPAN+")"));
  assertEquals(List.of("spec,spec-2,spec-3 - UNRESOLVED_SAME_APPROVAL_TIME 12:05-12:10"),strings(Specifications.overlaps(tied),
   "concat(array_join(specificationIds,','),' ',coalesce(specificationId,'-'),' ',resolution,' ',"+SPAN+")"));
  Row t=unitA(tied);
  assertEquals(300L,(long)t.getAs("unspecifiedSeconds"));assertEquals(300L,(long)t.getAs("coveredSeconds"));
  assertEquals(List.of("EXCURSION_ABOVE_ALLOWANCE","MISSING_COVERAGE","MISSING_SPECIFICATION"),reasons(t));
 }
 @Test void syntheticThresholdsApplyOnlyWhereAllowed()throws Exception {
  assertTrue((boolean)unitA(Simulator.initial()).getAs("syntheticThresholds"));
  Map<String,Object> real=spec("spec-real","2026-01-01T11:00:00Z",0,10,8.0);real.remove("synthetic");
  s.conf().set(Specifications.ALLOW_SYNTHETIC,false);
  try {
   Row a=unitA(Simulator.initial());
   assertEquals("INSUFFICIENT_DATA",a.getAs("assessment"));assertNull(a.getAs("specificationIds"));
   assertEquals(600L,(long)a.getAs("unspecifiedSeconds"));assertEquals(List.of("MISSING_COVERAGE","MISSING_SPECIFICATION"),reasons(a));
   // An approved (non-synthetic) specification is used, and the synthetic one does not compete with it.
   Row r=unitA(initialPlus(real));
   assertEquals("QUALITY_REVIEW",r.getAs("assessment"));assertEquals(List.of("spec-real"),r.getList(r.fieldIndex("specificationIds")));
   assertFalse((boolean)r.getAs("syntheticThresholds"));
  } finally {s.conf().set(Specifications.ALLOW_SYNTHETIC,true);}
 }
 @Test void temperatureUnitsAreNormalizedToCelsius()throws Exception {
  List<Map<String,Object>> readings=List.of(with(6,"eventId","c","temperatureC",null,"temperature",12.0,"temperatureUnit","C"),
   with(6,"eventId","f","temperatureC",null,"temperature",53.6,"temperatureUnit","F"),
   with(6,"eventId","k","temperatureC",null,"temperature",285.15,"temperatureUnit","K"));
  Dataset<Row> parsed=valid(readings);
  assertEquals(List.of("c 12.0 C","f 12.0 F","k 12.0 K"),strings(parsed,"concat(eventId,' ',temperatureC,' ',temperatureUnit)"));
  // The hot reading corrected to the same value in °F gives the same exposure.
  Row a=unitA(initialPlus(with(6,"revision",2,"temperatureC",null,"temperature",53.6,"temperatureUnit","F")));
  assertEquals(600L,(long)a.getAs("excursionSeconds"));assertEquals("QUALITY_REVIEW",a.getAs("assessment"));
  Row r=Measurements.readings(Model.canonical(valid(initialPlus(with(6,"revision",2,"temperatureC",null,"temperature",53.6,"temperatureUnit","F")))))
   .filter("eventId = 'reading-hot'").first();
  assertEquals(12.0,(double)r.getAs("temperatureC"));assertEquals(53.6,(double)r.getAs("reportedTemperature"));assertEquals("F",r.getAs("reportedUnit"));
 }
 @Test void calibrationValidityAndUncertaintyLimitTheConclusion()throws Exception {
  Row cool=unitA(initialPlus(with(6,"revision",2,"temperatureC",5.0)));
  assertEquals("NO_OBSERVED_EXCURSION",cool.getAs("assessment"));assertEquals(List.of(),reasons(cool));
  assertEquals(List.of("calibration-1"),cool.getList(cool.fieldIndex("calibrationIds")));assertEquals(0.5,(double)cool.getAs("maxUncertaintyC"));
  // 7.8 °C is inside 2-8 °C but within the ±0.5 °C uncertainty of the limit for 600 s, more than the 60 s allowance.
  Row near=unitA(initialPlus(with(6,"revision",2,"temperatureC",7.8)));
  assertEquals(0L,(long)near.getAs("excursionSeconds"));assertEquals(600L,(long)near.getAs("possibleExcursionSeconds"));
  assertEquals("INSUFFICIENT_DATA",near.getAs("assessment"));assertEquals(List.of("WITHIN_MEASUREMENT_UNCERTAINTY"),reasons(near));
  // The calibration is revised to expire at 12:06: four minutes of the reading are uncalibrated.
  List<Map<String,Object>> expired=initialPlus(with(6,"revision",2,"temperatureC",5.0),with(12,"revision",2,"endTime",t(6)));
  Row e=unitA(expired);
  assertEquals(240L,(long)e.getAs("uncalibratedSeconds"));assertEquals(List.of("UNCALIBRATED_MEASUREMENT"),reasons(e));
  Row q=Measurements.readings(Model.canonical(valid(expired))).filter("eventId = 'reading-hot'").first();
  assertEquals("PARTIALLY_CALIBRATED",q.getAs("calibration"));assertEquals(360L,(long)q.getAs("calibratedSeconds"));
  // An uncalibrated out-of-range reading still counts as exposure.
  Row hot=unitA(Simulator.initial().subList(0,12));
  assertEquals("QUALITY_REVIEW",hot.getAs("assessment"));assertEquals(600L,(long)hot.getAs("uncalibratedSeconds"));assertNull(hot.getAs("calibrationIds"));
 }
 @Test void pointReadingsAreNeverWidenedAndSamplingGapsAreReported()throws Exception {
  // Cool readings 12:01-12:04 and 12:07-12:10, and a spot reading at 12:05.
  Map<String,Object> late=Simulator.event("reading-late","READING","sensorId","SENSOR-1","startTime",t(7),"endTime",t(10),"temperatureC",5.0);
  List<Map<String,Object>> base=initialPlus(with(6,"revision",2,"startTime",t(1),"endTime",t(4),"temperatureC",5.0),late);
  Map<String,Object> spot=Simulator.event("spot","READING","sensorId","SENSOR-1","startTime",t(5),"endTime",t(5),"temperatureC",15.0);
  List<Map<String,Object>> hot=new ArrayList<>(base);hot.add(spot);
  Dataset<Row> events=Model.canonical(valid(hot));
  Row a=unitA(events);
  assertEquals(360L,(long)a.getAs("coveredSeconds"),"the spot reading covers no time");
  assertEquals(0L,(long)a.getAs("excursionSeconds"));assertEquals(1L,(long)a.getAs("outOfRangePoints"));
  assertEquals("QUALITY_REVIEW",a.getAs("assessment"));assertEquals(List.of("OUT_OF_RANGE_POINT","MISSING_COVERAGE"),reasons(a));
  assertTrue(a.getList(a.fieldIndex("readingIds")).contains("spot"));
  assertEquals(List.of("assign 12:00-12:01 60","assign 12:04-12:07 180"),
   strings(Measurements.samplingGaps(events),"concat(assignmentId,' ',"+SPAN+",' ',seconds)"),"the spot reading fills no gap");
  Row p=Measurements.readings(events).filter("eventId = 'spot'").first();
  assertTrue((boolean)p.getAs("point"));assertEquals("CALIBRATED",p.getAs("calibration"));
  // In range, the spot reading changes nothing.
  List<Map<String,Object>> cool=new ArrayList<>(base);Map<String,Object> inRange=new LinkedHashMap<>(spot);inRange.put("temperatureC",5.0);cool.add(inRange);
  Row c=unitA(cool);
  assertEquals("INSUFFICIENT_DATA",c.getAs("assessment"));assertEquals(360L,(long)c.getAs("coveredSeconds"));assertEquals(0L,(long)c.getAs("outOfRangePoints"));
 }
}

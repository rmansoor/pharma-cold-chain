package io.coldchain;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.coldchain.ModelTest.with;
import static io.coldchain.TimelineTest.*;
class ExposureTest {
 static SparkSession s;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-exposure-tests");s.sparkContext().setLogLevel("ERROR");
  s.conf().set(Specifications.ALLOW_SYNTHETIC,true);}
 @AfterAll static void stop(){s.stop();}
 // Indexes into Simulator.initial(): 1 UNIT-A in BOX-A, 3 SENSOR-1 assigned to BOX-A, 6 hot reading (12 °C), all 12:00-12:10,
 // under a 2-8 °C specification with a 60 s allowance.
 static String t(int minute){return LineageTest.t(minute);}
 static final Map<String,String> COLUMNS=Map.of("EXPECTED","expectedSeconds","COVERED","coveredSeconds","EXCURSION","excursionSeconds",
  "POSSIBLE_EXCURSION","possibleExcursionSeconds","MISSING","missingSeconds","UNCALIBRATED","uncalibratedSeconds",
  "UNSPECIFIED","unspecifiedSeconds","MISSING_CUSTODY","missingCustodySeconds");
 static Model.Result exposure(Map<String,Object>... extra)throws Exception {
  Model.Result r=Model.exposure(s,Model.canonical(valid(initialPlus(extra))));
  // Every Gold duration is the length of its persisted intervals, which are disjoint and do not touch.
  for(Row a:r.assessments().collectAsList()) for(var c:COLUMNS.entrySet()) {
   List<Row> rows=r.intervals().filter(col("productId").equalTo(a.<String>getAs("productId")).and(col("metric").equalTo(c.getKey())))
    .orderBy("startTime").collectAsList();
   long total=0;Object previousEnd=null;
   for(Row i:rows) {
    total+=i.<Long>getAs("seconds");
    if(previousEnd!=null)assertTrue(((java.sql.Timestamp)i.getAs("startTime")).after((java.sql.Timestamp)previousEnd),c.getKey()+" islands are disjoint");
    previousEnd=i.getAs("endTime");
   }
   assertEquals((long)a.<Long>getAs(c.getValue()),total,a.getAs("productId")+" "+c.getValue());
  }
  return r;
 }
 static final String SPAN="concat(date_format(startTime,'HH:mm'),'-',date_format(endTime,'HH:mm'))";
 // UNIT-A's intervals of one metric as "HH:mm-HH:mm eventId@revision,...".
 static List<String> intervals(Model.Result r,String metric) {
  return strings(r.intervals().filter("productId = 'UNIT-A' AND metric = '"+metric+"'"),
   "concat("+SPAN+",' ',array_join(transform(sources,x -> concat(x.eventId,'@',x.revision)),','))");
 }
 static Row unitA(Model.Result r){return product(r.assessments(),"UNIT-A");}
 @Test void boundariesAreHalfOpen()throws Exception {
  // Readings that touch at 12:05 form one covered interval; a reading that starts when the placement ends adds nothing.
  Model.Result r=exposure(with(6,"revision",2,"endTime",t(5)),with(6,"eventId","reading-2","startTime",t(5)),
   with(6,"eventId","reading-after","startTime",t(10),"endTime",t(15)));
  Row a=unitA(r);
  assertEquals(600L,(long)a.getAs("coveredSeconds"));assertEquals(600L,(long)a.getAs("excursionSeconds"));
  assertEquals(List.of("12:00-12:10 assign@1,calibration-1@1,place-A@1,reading-2@1,reading-hot@2,spec@1"),intervals(r,"CALIBRATED"));
  assertEquals(List.of("reading-2","reading-hot"),a.getList(a.fieldIndex("readingIds")),"reading-after shares only the instant 12:10");
  // A point reading at the placement's end instant is outside [12:00,12:10); one at its start is inside.
  Model.Result points=exposure(with(6,"eventId","point-end","startTime",t(10),"endTime",t(10)),
   with(6,"eventId","point-start","startTime",t(0),"endTime",t(0)));
  assertEquals(List.of("12:00-12:00 assign@1,place-A@1,point-start@1,spec@1"),intervals(points,"OUT_OF_RANGE_POINT"));
  assertEquals(1L,(long)unitA(points).getAs("outOfRangePoints"));
 }
 @Test void duplicatesAndOverlappingSensorsAreCountedOnce()throws Exception {
  // The same reading under another ID, and a second sensor in BOX-A reading hot from 12:05 to 12:15.
  Model.Result r=exposure(with(6,"eventId","reading-copy"),with(3,"eventId","assign-2","sensorId","SENSOR-2"),
   with(6,"eventId","reading-2","sensorId","SENSOR-2","startTime",t(5),"endTime",t(15)));
  Row a=unitA(r);
  assertEquals(600L,(long)a.getAs("excursionSeconds"),"not 1200 or 1500");assertEquals(600L,(long)a.getAs("coveredSeconds"));
  assertEquals(List.of("12:00-12:10 assign@1,assign-2@1,place-A@1,reading-2@1,reading-copy@1,reading-hot@1,spec@1"),intervals(r,"EXCURSION"));
  // Three segments overlap 12:05-12:10, and each is kept with the sensor that measured it.
  assertEquals(List.of("12:00-12:10 SENSOR-1 reading-copy","12:00-12:10 SENSOR-1 reading-hot","12:05-12:10 SENSOR-2 reading-2"),
   strings(r.segments().filter("productId = 'UNIT-A'"),"concat("+SPAN+",' ',sensorId,' ',readingId)"));
  // SENSOR-2 has no calibration, but SENSOR-1's calibrated readings cover the same time, so nothing is uncalibrated.
  assertEquals(0L,(long)a.getAs("uncalibratedSeconds"));
 }
 @Test void gapsArePersistedAsIntervals()throws Exception {
  // No reading from 12:03 to 12:06; custody is missing after 12:08; the specification ends at 12:09.
  Model.Result r=exposure(with(6,"revision",2,"endTime",t(3)),with(6,"eventId","reading-2","startTime",t(6)),
   with(11,"revision",2,"endTime",t(8)),with(0,"revision",2,"endTime",t(9)));
  Row a=unitA(r);
  // A segment needs a specification, so the unspecified minute is unmeasured too: both are missing coverage, not exposure.
  assertEquals(List.of("12:03-12:06 place-A@1","12:09-12:10 place-A@1"),intervals(r,"MISSING"));assertEquals(240L,(long)a.getAs("missingSeconds"));
  assertEquals(List.of("12:00-12:03","12:06-12:09"),strings(r.intervals().filter("productId = 'UNIT-A' AND metric = 'COVERED'"),SPAN));
  assertEquals(List.of("12:08-12:10 place-A@1"),intervals(r,"MISSING_CUSTODY"));
  assertEquals(List.of("12:09-12:10 place-A@1"),intervals(r,"UNSPECIFIED"));
  assertEquals(360L,(long)a.getAs("excursionSeconds"));
 }
 @Test void aSplitKeepsTheTotalAndShowsEachSide()throws Exception {
  // SENSOR-1 leaves BOX-A at 12:05 and SENSOR-2 (uncalibrated) takes over with an in-range reading.
  Model.Result r=exposure(with(3,"revision",2,"endTime",t(5)),with(3,"eventId","assign-2","sensorId","SENSOR-2","startTime",t(5)),
   with(6,"eventId","reading-2","sensorId","SENSOR-2","temperatureC",5.0));
  Row a=unitA(r);
  assertEquals(List.of("12:00-12:05 assign@2,calibration-1@1,place-A@1,reading-hot@1,spec@1"),intervals(r,"CALIBRATED"));
  assertEquals(List.of("12:00-12:05 assign@2,place-A@1,reading-hot@1,spec@1"),intervals(r,"EXCURSION"));
  assertEquals(List.of("12:05-12:10 assign-2@1,place-A@1,reading-2@1,spec@1"),intervals(r,"UNCALIBRATED"));
  assertEquals(List.of("12:00-12:05 assign 2 reading-hot true 300","12:05-12:10 assign-2 1 reading-2 false 300"),
   strings(r.segments().filter("productId = 'UNIT-A'"),"concat("+SPAN+",' ',assignmentId,' ',"
    +"filter(sources,x -> x.eventId = assignmentId)[0].revision,' ',readingId,' ',outOfRange,' ',seconds)"));
  assertEquals(600L,(long)a.getAs("coveredSeconds"));assertEquals(300L,(long)a.getAs("excursionSeconds"));
 }
 @Test void evidenceNamesTheExactRevisionsUsed()throws Exception {
  // Revision 3 of the hot reading (revision 2 never arrived) restates it; only the revision used is evidence.
  Model.Result r=exposure(with(6,"revision",3));
  Row hot=r.intervals().filter("productId = 'UNIT-A' AND metric = 'EXCURSION'").selectExpr("filter(sources,x -> x.eventId = 'reading-hot')[0] s").first().getStruct(0);
  assertEquals(3L,(long)hot.getAs("revision"));assertEquals("coldchain.sensors",hot.getAs("topic"));assertNotNull(hot.getAs("offset"));
  String hash=Model.canonical(valid(initialPlus(with(6,"revision",3)))).filter("eventId = 'reading-hot'").first().getAs("payloadHash");
  assertEquals(hash,hot.getAs("payloadHash"));
  Row a=unitA(r);
  assertEquals(List.of("ASSIGNMENT assign@1","CALIBRATION calibration-1@1","CUSTODY custody-A-1@1","CUSTODY custody-A-2@1",
   "PLACEMENT place-A@1","READING reading-hot@3","SPEC spec@1"),
   strings(r.assessments().filter("productId = 'UNIT-A'").selectExpr("explode(sourceRevisions) x"),"concat(x.kind,' ',x.eventId,'@',x.revision)"));
  // A new revision changes the evidence and so the assessment hash, even when no number changes.
  Row before=unitA(exposure());
  assertEquals((long)before.<Long>getAs("excursionSeconds"),(long)a.<Long>getAs("excursionSeconds"));
  assertNotEquals(before.<String>getAs("assessmentHash"),a.<String>getAs("assessmentHash"));
 }
}

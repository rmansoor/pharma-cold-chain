package io.coldchain;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
class LineageTest {
 static SparkSession s;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-lineage-tests");s.sparkContext().setLogLevel("ERROR");}
 @AfterAll static void stop(){s.stop();}
 static String t(int minute){return String.format("2026-01-01T12:%02d:00Z",minute);}
 static Map<String,Object> pack(String id,String child,String childLevel,String parent,String parentLevel,int from,int to) {
  Map<String,Object> e=Simulator.event(id,"PACKING","childId",child,"childLevel",childLevel,"parentId",parent,"parentLevel",parentLevel,
   "startTime",t(from),"endTime",t(to));
  if(childLevel.equals("UNIT")){e.put("productCode","DRUG-DEMO");e.put("lotId","LOT-1");}
  return e;
 }
 static Dataset<Row> events(List<Map<String,Object>> input)throws Exception {
  List<String> json=new ArrayList<>();for(var e:input)json.add(Simulator.JSON.writeValueAsString(e));
  Dataset<Row> parsed=ModelTest.parseRaw(json,expr(ModelTest.TOPIC));
  assertEquals(0,parsed.filter("NOT valid").count(),"test events must satisfy the contract");
  return Model.canonical(parsed.filter("valid"));
 }
 static Dataset<Row> containers(Dataset<Row> events) {
  Dataset<Row> edges=Lineage.edges(events);return Lineage.unitContainers(edges,Lineage.anomalies(edges));
 }
 // Serialized units in a container at any instant of [from,to).
 static Set<String> unitsIn(Dataset<Row> containers,String container,int from,int to) {
  return new TreeSet<>(containers.filter(col("containerId").equalTo(container)
   .and(col("startTime").lt(to_timestamp(lit(t(to))))).and(col("endTime").gt(to_timestamp(lit(t(from))))))
   .select("productId").distinct().as(Encoders.STRING()).collectAsList());
 }
 // Pallet P1 in container X holds cartons C1 (U1, U2) and C2 (U3, U4). At 12:10 the pallet is split: C2 moves to pallet P2 in Y.
 static List<Map<String,Object>> split() {
  return new ArrayList<>(List.of(pack("u1","U1","UNIT","C1","CARTON",0,20),pack("u2","U2","UNIT","C1","CARTON",0,20),
   pack("u3","U3","UNIT","C2","CARTON",0,20),pack("u4","U4","UNIT","C2","CARTON",0,20),
   pack("c1","C1","CARTON","P1","PALLET",0,20),pack("c2-before","C2","CARTON","P1","PALLET",0,10),
   pack("c2-after","C2","CARTON","P2","PALLET",10,20),
   pack("p1","P1","PALLET","X","CONTAINER",0,20),pack("p2","P2","PALLET","Y","CONTAINER",0,20)));
 }
 @Test void palletSplitIdentifiesExactlyTheAffectedUnitsAtTheBoundary()throws Exception {
  Dataset<Row> c=containers(events(split()));
  assertEquals(Set.of("U1","U2","U3","U4"),unitsIn(c,"X",0,10));
  assertEquals(Set.of("U1","U2","U3","U4"),unitsIn(c,"X",9,10),"the minute before the split");
  assertEquals(Set.of("U1","U2"),unitsIn(c,"X",10,11),"intervals are half-open: C2 has left X at 12:10");
  assertEquals(Set.of("U1","U2"),unitsIn(c,"X",10,20));
  assertEquals(Set.of(),unitsIn(c,"Y",0,10));
  assertEquals(Set.of("U3","U4"),unitsIn(c,"Y",10,20));
  Row u3=c.filter("productId = 'U3' AND containerId = 'X'").first();
  assertEquals(List.of("U3","C2","P1","X"),u3.getList(u3.fieldIndex("path")));
  assertEquals(List.of("u3","c2-before","p1"),u3.getList(u3.fieldIndex("lineageIds")));
  // A hot reading in X after the split puts only the units still in X under quality review.
  List<Map<String,Object>> input=split();
  input.add(Simulator.event("spec","SPEC","productCode","DRUG-DEMO","startTime",t(0),"endTime",t(20),"minC",2.0,"maxC",8.0,"maxExcursionSeconds",60));
  input.add(Simulator.event("assign-x","ASSIGNMENT","sensorId","S1","containerId","X","startTime",t(0),"endTime",t(20)));
  input.add(Simulator.event("cool","READING","sensorId","S1","startTime",t(0),"endTime",t(15),"temperatureC",5.0));
  input.add(Simulator.event("hot","READING","sensorId","S1","startTime",t(15),"endTime",t(20),"temperatureC",12.0));
  Dataset<Row> gold=Model.assessments(s,events(input));
  assertEquals(List.of("U1","U2"),gold.filter("assessment = 'QUALITY_REVIEW'").orderBy("productId").select("productId").as(Encoders.STRING()).collectAsList());
  Row a3=gold.filter("productId = 'U3'").first();
  assertEquals(0L,(long)a3.getAs("excursionSeconds"));assertEquals(600L,(long)a3.getAs("coveredSeconds"));
  assertEquals(1200L,(long)a3.getAs("expectedSeconds"));assertEquals("INSUFFICIENT_DATA",a3.getAs("assessment"),"Y has no sensor");
  Row a1=gold.filter("productId = 'U1'").first();
  assertEquals(List.of("c1","p1","u1"),a1.getList(a1.fieldIndex("lineageIds")));
 }
 @Test void mergeAndRepackFollowMembershipOverTime()throws Exception {
  // Merge: at 12:10 carton C2 leaves pallet P2 (in Y) and joins P1 (in X). Repack: at 12:05 U1 moves from C1 to C3, which is on P2.
  Dataset<Row> c=containers(events(List.of(pack("u1-c1","U1","UNIT","C1","CARTON",0,5),pack("u1-c3","U1","UNIT","C3","CARTON",5,20),
   pack("u3","U3","UNIT","C2","CARTON",0,20),pack("c1","C1","CARTON","P1","PALLET",0,20),pack("c3","C3","CARTON","P2","PALLET",0,20),
   pack("c2-p2","C2","CARTON","P2","PALLET",0,10),pack("c2-p1","C2","CARTON","P1","PALLET",10,20),
   pack("p1","P1","PALLET","X","CONTAINER",0,20),pack("p2","P2","PALLET","Y","CONTAINER",0,20))));
  assertEquals(List.of("X 12:00-12:05","Y 12:05-12:20"),spans(c,"U1"));
  assertEquals(List.of("Y 12:00-12:10","X 12:10-12:20"),spans(c,"U3"));
  // A unit whose carton is not on any pallet yet has no known container for that time.
  Dataset<Row> loose=containers(events(List.of(pack("u9","U9","UNIT","C9","CARTON",0,20),pack("c9","C9","CARTON","P1","PALLET",5,20),
   pack("p1","P1","PALLET","X","CONTAINER",0,20))));
  assertEquals(List.of("X 12:05-12:20"),spans(loose,"U9"));
 }
 static List<String> spans(Dataset<Row> containers,String unit) {
  return containers.filter(col("productId").equalTo(unit)).orderBy("startTime")
   .selectExpr("concat(containerId,' ',date_format(startTime,'HH:mm'),'-',date_format(endTime,'HH:mm'))").as(Encoders.STRING()).collectAsList();
 }
 @Test void contradictionsLevelMismatchesAndCyclesAreReportedAndNotResolved()throws Exception {
  List<Map<String,Object>> input=new ArrayList<>(List.of(
   // U5 is in C1 and C2 at once from 12:05 to 12:10.
   pack("u5-c1","U5","UNIT","C1","CARTON",0,10),pack("u5-c2","U5","UNIT","C2","CARTON",5,15),
   // U6 is packed in C1 but also placed straight into Y.
   pack("u6","U6","UNIT","C1","CARTON",0,20),
   Simulator.event("u6-direct","PLACEMENT","productId","U6","productCode","DRUG-DEMO","lotId","LOT-1","containerId","Y","startTime",t(15),"endTime",t(20)),
   // A and B each claim to hold the other.
   pack("a-in-b","A","CARTON","B","PALLET",0,10),pack("b-in-a","B","CARTON","A","PALLET",0,10),
   // A clean chain is unaffected.
   pack("u7","U7","UNIT","C1","CARTON",0,20),pack("c1","C1","CARTON","P1","PALLET",0,20),pack("c2","C2","CARTON","P1","PALLET",0,20),
   pack("p1","P1","PALLET","X","CONTAINER",0,20)));
  Dataset<Row> edges=Lineage.edges(events(input)),anomalies=Lineage.anomalies(edges);
  List<String> found=anomalies.orderBy("anomaly","subjectId").selectExpr("concat(anomaly,' ',subjectId,' ',array_join(eventIds,','),' ',"
   +"coalesce(date_format(startTime,'HH:mm'),'-'),'-',coalesce(date_format(endTime,'HH:mm'),'-'))").as(Encoders.STRING()).collectAsList();
  assertEquals(List.of("CONTRADICTORY_PARENT U5 u5-c1,u5-c2 12:05-12:10","CONTRADICTORY_PARENT U6 u6,u6-direct 12:15-12:20",
   "CYCLE A a-in-b,b-in-a 12:00-12:10","LEVEL_MISMATCH A a-in-b,b-in-a ---","LEVEL_MISMATCH B a-in-b,b-in-a ---"),found);
  Dataset<Row> c=Lineage.unitContainers(edges,anomalies);
  assertEquals(Set.of("U7"),new HashSet<>(c.select("productId").as(Encoders.STRING()).collectAsList()));
  Row m=Lineage.memberships(edges,anomalies).filter("eventId = 'a-in-b'").first();
  assertEquals(List.of("CYCLE","LEVEL_MISMATCH"),m.getList(m.fieldIndex("anomalies")));
  // U5 stays visible in Gold as missing coverage instead of being placed arbitrarily.
  input.add(Simulator.event("spec","SPEC","productCode","DRUG-DEMO","startTime",t(0),"endTime",t(20),"minC",2.0,"maxC",8.0,"maxExcursionSeconds",60));
  input.add(Simulator.event("assign-x","ASSIGNMENT","sensorId","S1","containerId","X","startTime",t(0),"endTime",t(20)));
  input.add(Simulator.event("cool","READING","sensorId","S1","startTime",t(0),"endTime",t(20),"temperatureC",5.0));
  input.add(Simulator.event("cal-s1","CALIBRATION","sensorId","S1","startTime",t(0),"endTime",t(20),"uncertaintyC",0.5));
  Dataset<Row> gold=Model.assessments(s,events(input));
  Row u5=gold.filter("productId = 'U5'").first();
  assertEquals("INSUFFICIENT_DATA",u5.getAs("assessment"));assertEquals(0L,(long)u5.getAs("coveredSeconds"));assertEquals(900L,(long)u5.getAs("expectedSeconds"));
  assertEquals("NO_OBSERVED_EXCURSION",gold.filter("productId = 'U7'").first().getAs("assessment"));
  // Reversed membership at different times is not a cycle, but A and B still change level.
  Dataset<Row> later=Lineage.edges(events(List.of(pack("a-in-b","A","CARTON","B","PALLET",0,10),pack("b-in-a","B","CARTON","A","PALLET",10,20))));
  assertEquals(List.of("LEVEL_MISMATCH","LEVEL_MISMATCH"),Lineage.anomalies(later).orderBy("subjectId").select("anomaly").as(Encoders.STRING()).collectAsList());
 }
}

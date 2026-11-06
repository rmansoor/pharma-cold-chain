package io.coldchain;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
class ModelTest {
 static SparkSession s;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-tests");s.sparkContext().setLogLevel("ERROR");}
 @AfterAll static void stop(){s.stop();}
 Dataset<Row> parse(List<Map<String,Object>> input)throws Exception {
  List<String> json=new ArrayList<>();for(var e:input)json.add(Simulator.JSON.writeValueAsString(e));
  Dataset<Row> raw=s.createDataset(json,Encoders.STRING()).toDF("raw")
   .withColumn("topic",expr("CASE get_json_object(raw,'$.kind') WHEN 'READING' THEN 'coldchain.sensors' WHEN 'SPEC' THEN 'coldchain.specifications' WHEN 'INVENTORY' THEN 'coldchain.inventory' ELSE 'coldchain.traceability' END"))
   .withColumn("kafkaTime",current_timestamp()).withColumn("partition",lit(0)).withColumn("offset",monotonically_increasing_id());
  return Model.parse(raw);
 }
 @Test void correctionMovesExposureAndDeduplicates()throws Exception {
  List<Map<String,Object>> initial=new ArrayList<>(Simulator.initial());initial.add(initial.get(6));
  Dataset<Row> before=Model.assessments(s,Model.canonical(parse(initial).filter("valid")));
  assertEquals(600L,(Long)before.filter("productId='UNIT-A'").first().getAs("excursionSeconds"));
  Map<String,Object> corrected=new LinkedHashMap<>(initial.get(3));corrected.put("revision",2);corrected.put("containerId","BOX-B");initial.add(corrected);
  Dataset<Row> after=Model.assessments(s,Model.canonical(parse(initial).filter("valid")));
  assertEquals("QUALITY_REVIEW",after.filter("productId='UNIT-B'").first().getAs("assessment"));
  assertEquals("INSUFFICIENT_DATA",after.filter("productId='UNIT-A'").first().getAs("assessment"));
 }
 @Test void overlappingReadingsCountUnionNotSum()throws Exception {
  List<Map<String,Object>> input=new ArrayList<>(Simulator.initial());var overlapping=new LinkedHashMap<>(input.get(6));overlapping.put("eventId","overlap");input.add(overlapping);
  Row a=Model.assessments(s,Model.canonical(parse(input).filter("valid"))).filter("productId='UNIT-A'").first();
  assertEquals(600L,(Long)a.getAs("coveredSeconds"));assertEquals(600L,(Long)a.getAs("excursionSeconds"));
 }
 @Test void invalidAndConflictingContractsAreExcluded()throws Exception {
  List<Map<String,Object>> input=new ArrayList<>(Simulator.initial());var broken=new LinkedHashMap<>(input.get(6));broken.put("eventId","invalid");broken.put("endTime",broken.get("startTime"));input.add(broken);
  Dataset<Row> parsed=parse(input);assertEquals(1,parsed.filter("NOT valid").count());
  var conflict=new LinkedHashMap<>(input.get(3));conflict.put("containerId","OTHER");input.add(conflict);
  assertEquals(0,Model.canonical(parse(input).filter("valid")).filter("eventId='assign'").count());
 }
}

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
 static final String TOPIC="CASE get_json_object(raw,'$.kind') WHEN 'READING' THEN 'coldchain.sensors' WHEN 'SPEC' THEN 'coldchain.specifications' WHEN 'INVENTORY' THEN 'coldchain.inventory' ELSE 'coldchain.traceability' END";
 Dataset<Row> parseRaw(List<String> json,Column topic) {
  Dataset<Row> raw=s.createDataset(json,Encoders.STRING()).toDF("raw").withColumn("topic",topic)
   .withColumn("kafkaTime",current_timestamp()).withColumn("partition",lit(0)).withColumn("offset",monotonically_increasing_id());
  return Model.parse(raw);
 }
 Dataset<Row> parse(List<Map<String,Object>> input)throws Exception {
  List<String> json=new ArrayList<>();for(var e:input)json.add(Simulator.JSON.writeValueAsString(e));
  return parseRaw(json,expr(TOPIC));
 }
 boolean valid(String json){return parseRaw(List.of(json),expr(TOPIC)).first().getAs("valid");}
 static Map<String,Object> with(int base,Object... fields) {
  Map<String,Object> e=new LinkedHashMap<>(Simulator.initial().get(base));
  for(int i=0;i<fields.length;i+=2)e.put((String)fields[i],fields[i+1]);return e;
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
 @Test void publishedExamplesSatisfyTheContract()throws Exception {
  List<String> json=new ArrayList<>();
  try(var files=java.nio.file.Files.list(java.nio.file.Path.of("contracts/examples"))) {for(var f:files.sorted().toList())json.add(java.nio.file.Files.readString(f));}
  Dataset<Row> parsed=parseRaw(json,expr(TOPIC));
  assertEquals(0,parsed.filter("NOT valid").count());
  assertEquals(Set.of("READING","PLACEMENT","ASSIGNMENT","SPEC","INVENTORY"),
   new HashSet<>(parsed.select("kind").as(Encoders.STRING()).collectAsList()));
 }
 @Test void contractViolationsAreInvalid()throws Exception {
  // Indexes into Simulator.initial(): 0 spec, 1 placement, 3 assignment, 4 inventory, 6 reading.
  Map<String,Map<String,Object>> cases=new LinkedHashMap<>();
  cases.put("blank eventId",with(6,"eventId","  "));
  cases.put("blank containerId",with(3,"containerId",""));
  cases.put("blank lotId",with(1,"lotId"," "));
  cases.put("blank location",with(4,"location","\t"));
  cases.put("revision zero",with(6,"revision",0));
  cases.put("fractional revision",with(6,"revision",1.5));
  cases.put("unknown kind",with(6,"kind","TELEMETRY"));
  cases.put("unknown status",with(4,"status","LOST"));
  cases.put("empty interval",with(6,"endTime","2026-01-01T12:00:00Z"));
  cases.put("reversed interval",with(1,"startTime","2026-01-01T12:20:00Z"));
  cases.put("missing endTime",with(3,"endTime",null));
  cases.put("subsecond bound",with(6,"startTime","2026-01-01T12:00:00.500Z"));
  cases.put("reading ends in future",with(6,"endTime","2099-01-01T00:00:00Z"));
  cases.put("event time in future",with(4,"eventTime","2099-01-01T00:00:00Z"));
  cases.put("temperature out of range",with(6,"temperatureC",150.0));
  cases.put("inverted spec",with(0,"minC",9.0));
  cases.put("negative allowance",with(0,"maxExcursionSeconds",-1));
  for(var c:cases.entrySet())assertFalse(valid(Simulator.JSON.writeValueAsString(c.getValue())),c.getKey());
  String reading=Simulator.JSON.writeValueAsString(Simulator.initial().get(6)),spec=Simulator.JSON.writeValueAsString(Simulator.initial().get(0));
  assertTrue(valid(reading));assertTrue(valid(spec));
  assertFalse(valid(reading.replace("12.0","NaN")),"NaN temperature");
  assertFalse(valid(spec.replace("8.0","Infinity")),"infinite maxC");
  assertFalse(valid(spec.replace("2.0","-Infinity")),"infinite minC");
  assertFalse(valid("{broken"),"malformed JSON");
  assertFalse((boolean)parseRaw(List.of(reading),lit("coldchain.traceability")).first().getAs("valid"),"reading on wrong topic");
 }
 @Test void formattingDoesNotConflictButMeaningDoes()throws Exception {
  List<Map<String,Object>> input=new ArrayList<>(Simulator.initial());
  List<String> json=new ArrayList<>();for(var e:input)json.add(Simulator.JSON.writeValueAsString(e));
  // Same assignment: reordered keys, whitespace, explicit offset and milliseconds.
  json.add("{ \"startTime\" : \"2026-01-01T12:00:00.000+00:00\", \"kind\":\"ASSIGNMENT\", \"eventId\":\"assign\", \"revision\":1,\n"
   +" \"containerId\":\"BOX-A\", \"sensorId\":\"SENSOR-1\", \"endTime\":\"2026-01-01T12:10:00Z\", \"eventTime\":\"2026-01-01T12:00:00Z\" }");
  // Same spec with integral numbers instead of decimals.
  json.add(Simulator.JSON.writeValueAsString(with(0,"minC",2,"maxC",8)));
  Dataset<Row> same=parseRaw(json,expr(TOPIC)).filter("valid");
  assertEquals(0,Model.conflicts(same).count());
  assertEquals(600L,(Long)Model.assessments(s,Model.canonical(same)).filter("productId='UNIT-A'").first().getAs("excursionSeconds"));
  json.add(Simulator.JSON.writeValueAsString(with(3,"containerId","BOX-B")));
  json.add(Simulator.JSON.writeValueAsString(with(4,"revision",2,"kind","PLACEMENT","productCode","DRUG-DEMO","lotId","LOT-1",
   "containerId","BOX-A","startTime","2026-01-01T12:00:00Z","endTime","2026-01-01T12:10:00Z")));
  Dataset<Row> conflicting=parseRaw(json,expr(TOPIC)).filter("valid");
  assertEquals(Set.of("assign","inventory-A"),new HashSet<>(Model.conflicts(conflicting).as(Encoders.STRING()).collectAsList()));
  assertEquals(0,Model.canonical(conflicting).filter("eventId IN ('assign','inventory-A')").count());
 }
}

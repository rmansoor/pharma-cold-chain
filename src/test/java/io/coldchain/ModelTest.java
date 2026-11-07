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
 static Dataset<Row> parseRaw(List<String> json,Column topic) {
  Dataset<Row> raw=SparkSession.active().createDataset(json,Encoders.STRING()).toDF("raw").withColumn("topic",topic)
   .withColumn("kafkaTime",current_timestamp()).withColumn("partition",lit(0)).withColumn("offset",monotonically_increasing_id())
   .withColumn("sourceSystem",lit("test")).withColumn("ingestedAt",current_timestamp());
  return Model.parse(raw);
 }
 static Dataset<Row> parse(List<Map<String,Object>> input)throws Exception {
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
  // UNIT-D is in BOX-A through CARTON-1 and PALLET-1.
  Row nested=before.filter("productId='UNIT-D'").first();
  assertEquals("QUALITY_REVIEW",nested.getAs("assessment"));assertEquals(600L,(Long)nested.getAs("excursionSeconds"));
  assertEquals(List.of("pack-carton-1","pack-pallet-1","pack-unit-D"),nested.getList(nested.fieldIndex("lineageIds")));
  Map<String,Object> corrected=new LinkedHashMap<>(initial.get(3));corrected.put("revision",2);corrected.put("containerId","BOX-B");initial.add(corrected);
  Dataset<Row> after=Model.assessments(s,Model.canonical(parse(initial).filter("valid")));
  assertEquals("QUALITY_REVIEW",after.filter("productId='UNIT-B'").first().getAs("assessment"));
  assertEquals("INSUFFICIENT_DATA",after.filter("productId='UNIT-A'").first().getAs("assessment"));
  Row moved=after.filter("productId='UNIT-D'").first();
  assertEquals("INSUFFICIENT_DATA",moved.getAs("assessment"));assertEquals(0L,(Long)moved.getAs("coveredSeconds"));assertNull(moved.getAs("lineageIds"));
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
  assertEquals(Set.of("READING","PLACEMENT","ASSIGNMENT","SPEC","INVENTORY","PACKING"),
   new HashSet<>(parsed.select("kind").as(Encoders.STRING()).collectAsList()));
 }
 List<String> reasons(String json,Column topic){Row r=parseRaw(List.of(json),topic).first();return r.getList(r.fieldIndex("reasons"));}
 @Test void contractViolationsAreInvalidWithReasonCodes()throws Exception {
  // Indexes into Simulator.initial(): 0 spec, 1 placement, 3 assignment, 4 inventory, 6 reading, 7 unit packing, 8 carton packing.
  String reading=Simulator.JSON.writeValueAsString(Simulator.initial().get(6)),spec=Simulator.JSON.writeValueAsString(Simulator.initial().get(0));
  Map<String,Object[]> cases=new LinkedHashMap<>();
  cases.put("blank eventId",new Object[]{with(6,"eventId","  "),"INVALID_ID:eventId"});
  cases.put("blank containerId",new Object[]{with(3,"containerId",""),"INVALID_ID:containerId"});
  cases.put("blank lotId",new Object[]{with(1,"lotId"," "),"INVALID_ID:lotId"});
  cases.put("blank location",new Object[]{with(4,"location","\t"),"INVALID_ID:location"});
  cases.put("revision zero",new Object[]{with(6,"revision",0),"INVALID_REVISION"});
  cases.put("fractional revision",new Object[]{with(6,"revision",1.5),"INVALID_REVISION"});
  cases.put("unknown kind",new Object[]{with(6,"kind","TELEMETRY"),"INVALID_KIND"});
  cases.put("unknown status",new Object[]{with(4,"status","LOST"),"INVALID_STATUS"});
  cases.put("empty interval",new Object[]{with(6,"endTime","2026-01-01T12:00:00Z"),"INVALID_INTERVAL"});
  cases.put("reversed interval",new Object[]{with(1,"startTime","2026-01-01T12:20:00Z"),"INVALID_INTERVAL"});
  cases.put("missing endTime",new Object[]{with(3,"endTime",null),"INVALID_INTERVAL"});
  cases.put("subsecond bound",new Object[]{with(6,"startTime","2026-01-01T12:00:00.500Z"),"FRACTIONAL_SECONDS"});
  cases.put("reading ends in future",new Object[]{with(6,"endTime","2099-01-01T00:00:00Z"),"READING_ENDS_AHEAD_OF_KAFKA"});
  cases.put("event time in future",new Object[]{with(4,"eventTime","2099-01-01T00:00:00Z"),"EVENT_TIME_AHEAD_OF_KAFKA"});
  cases.put("missing eventTime",new Object[]{with(4,"eventTime",null),"MISSING_EVENT_TIME"});
  cases.put("unparseable eventTime",new Object[]{with(4,"eventTime","yesterday"),"MISSING_EVENT_TIME"});
  cases.put("revision as text",new Object[]{with(6,"revision","two"),"INVALID_REVISION"});
  cases.put("temperature out of range",new Object[]{with(6,"temperatureC",150.0),"OUT_OF_RANGE:temperatureC"});
  cases.put("inverted spec",new Object[]{with(0,"minC",9.0),"SPEC_MIN_ABOVE_MAX"});
  cases.put("negative allowance",new Object[]{with(0,"maxExcursionSeconds",-1),"INVALID_EXCURSION_ALLOWANCE"});
  // NaN and Infinity are not JSON (RFC 8259), although Spark's lenient from_json would read them.
  cases.put("NaN temperature",new Object[]{reading.replace("12.0","NaN"),"MALFORMED_JSON"});
  cases.put("infinite maxC",new Object[]{spec.replace("8.0","Infinity"),"MALFORMED_JSON"});
  cases.put("infinite minC",new Object[]{spec.replace("2.0","-Infinity"),"MALFORMED_JSON"});
  cases.put("single-quoted JSON",new Object[]{reading.replace('"','\''),"MALFORMED_JSON"});
  cases.put("blank childId",new Object[]{with(8,"childId"," "),"INVALID_ID:childId"});
  cases.put("missing parentId",new Object[]{with(8,"parentId",null),"INVALID_ID:parentId"});
  cases.put("unknown childLevel",new Object[]{with(8,"childLevel","CASE"),"INVALID_LEVEL:childLevel"});
  cases.put("unit as parent",new Object[]{with(8,"parentLevel","UNIT"),"INVALID_LEVEL:parentLevel"});
  cases.put("pallet in carton",new Object[]{with(8,"childLevel","PALLET","parentLevel","CARTON"),"PARENT_NOT_ABOVE_CHILD"});
  cases.put("carton in itself",new Object[]{with(8,"parentId","CARTON-1"),"SELF_CONTAINED"});
  cases.put("packed unit without lot",new Object[]{with(7,"lotId",null),"INVALID_ID:lotId"});
  cases.put("packing without interval",new Object[]{with(8,"startTime",null),"INVALID_INTERVAL"});
  cases.put("malformed JSON",new Object[]{"{broken","MALFORMED_JSON"});
  cases.put("JSON array",new Object[]{"[1,2]","MALFORMED_JSON"});
  for(var c:cases.entrySet()) {
   Object event=c.getValue()[0];String json=event instanceof String t?t:Simulator.JSON.writeValueAsString(event);
   assertEquals(List.of(c.getValue()[1]),reasons(json,expr(TOPIC)),c.getKey());
  }
  assertEquals(List.of(),reasons(reading,expr(TOPIC)));assertEquals(List.of(),reasons(spec,expr(TOPIC)));
  // A carton or pallet needs no product code or lot.
  assertEquals(List.of(),reasons(Simulator.JSON.writeValueAsString(Simulator.initial().get(8)),expr(TOPIC)));
  assertEquals(List.of("WRONG_TOPIC"),reasons(reading,lit("coldchain.traceability")),"reading on wrong topic");
  // Independent faults are all reported.
  assertEquals(List.of("INVALID_REVISION","INVALID_ID:sensorId","OUT_OF_RANGE:temperatureC"),
   reasons(Simulator.JSON.writeValueAsString(with(6,"revision",0,"sensorId","","temperatureC",-273.0)),expr(TOPIC)));
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

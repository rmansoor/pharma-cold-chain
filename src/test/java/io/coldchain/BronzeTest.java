package io.coldchain;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.apache.spark.sql.*;
import static org.junit.jupiter.api.Assertions.*;
class BronzeTest {
 static SparkSession s;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-bronze-tests");s.sparkContext().setLogLevel("ERROR");}
 @AfterAll static void stop(){s.stop();}
 // One record in the Kafka source schema; header null means the record carries no headers.
 static String record(String topic,int partition,long offset,String value,String header) {
  String headers=header==null?"CAST(NULL AS array<struct<key:string,value:binary>>)"
   :"array(named_struct('key','source-system','value',CAST('"+header+"' AS BINARY)))";
  return "SELECT CAST('k-"+offset+"' AS BINARY) key, CAST('"+value+"' AS BINARY) value, '"+topic+"' topic, "+partition+" partition, "
   +offset+"L offset, TIMESTAMP'2026-01-01 12:00:00' timestamp, 0 timestampType, "+headers+" headers";
 }
 static Dataset<Row> batch(String... records){return s.sql(String.join(" UNION ALL ",records));}
 static Dataset<Row> read(Path dir){return s.read().format("delta").load(dir.toString()).orderBy("topic","partition","offset");}
 @Test void recordsProvenanceAndResolvesSourceSystem(@TempDir Path dir) {
  Pipeline.ingest(batch(record("coldchain.sensors",1,7,"{\"a\":1}","gateway-eu-3"),record("coldchain.inventory",0,2,"{\"b\":2}",null),
   record("coldchain.traceability",3,9,"{\"c\":3}","  ")),4,dir.toString());
  List<Row> rows=read(dir).collectAsList();assertEquals(3,rows.size());
  Row inventory=rows.get(0),sensors=rows.get(1),trace=rows.get(2);
  assertEquals("gateway-eu-3",sensors.getAs("sourceSystem"));assertTrue((boolean)sensors.getAs("sourceSystemDeclared"));
  assertEquals("warehouse-management",inventory.getAs("sourceSystem"));assertFalse((boolean)inventory.getAs("sourceSystemDeclared"));
  assertEquals("traceability",trace.getAs("sourceSystem"),"a blank header falls back to the topic owner");
  assertEquals("{\"a\":1}",sensors.getAs("raw"));assertEquals("k-7",sensors.getAs("kafkaKey"));
  assertEquals(1,(int)sensors.getAs("partition"));assertEquals(7L,(long)sensors.getAs("offset"));
  assertEquals(java.time.Instant.parse("2026-01-01T12:00:00Z"),((java.sql.Timestamp)sensors.getAs("kafkaTime")).toInstant());
  assertNotNull(sensors.getAs("ingestedAt"));assertEquals(4L,(long)sensors.getAs("ingestBatchId"));
 }
 @Test void replayedOffsetsAreNotDuplicatedButRepeatedPayloadsAreEvidence(@TempDir Path dir)throws Exception {
  String same="{\"eventId\":\"r1\"}";
  Dataset<Row> first=batch(record("coldchain.sensors",0,10,same,null),record("coldchain.sensors",0,11,"{\"eventId\":\"r2\"}",null));
  Pipeline.ingest(first,0,dir.toString());
  Object ingestedAt=read(dir).first().getAs("ingestedAt");
  Thread.sleep(20);
  // Restart replays batch 0 with the same offsets; the producer also retried r1 at a new offset.
  Pipeline.ingest(first,0,dir.toString());
  Pipeline.ingest(batch(record("coldchain.sensors",0,12,same,null)),1,dir.toString());
  List<Row> rows=read(dir).collectAsList();
  assertEquals(List.of(10L,11L,12L),rows.stream().map(r->(Long)r.getAs("offset")).toList());
  assertEquals(2,rows.stream().filter(r->same.equals(r.getAs("raw"))).count());
  assertEquals(ingestedAt,rows.get(0).getAs("ingestedAt"),"replay keeps the first ingestion time");
  assertEquals(List.of(0L,0L,1L),rows.stream().map(r->(Long)r.getAs("ingestBatchId")).toList());
 }
 @Test void appendsToBronzeWrittenBeforeProvenanceColumns(@TempDir Path dir) {
  s.sql("SELECT 'coldchain.sensors' topic, 0 partition, 1L offset, TIMESTAMP'2026-01-01 11:00:00' kafkaTime, '{}' raw")
   .write().format("delta").save(dir.toString());
  Pipeline.ingest(batch(record("coldchain.sensors",0,2,"{}","gw")),3,dir.toString());
  List<Row> rows=read(dir).collectAsList();assertEquals(2,rows.size());
  assertNull(rows.get(0).getAs("sourceSystem"));assertEquals("gw",rows.get(1).getAs("sourceSystem"));
 }
}

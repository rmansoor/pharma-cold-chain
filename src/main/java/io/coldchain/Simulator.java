package io.coldchain;
import java.time.Instant;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
public final class Simulator {
 static final ObjectMapper JSON=new ObjectMapper();
 public static Map<String,Object> event(String id,String kind,Object... fields) {
  Map<String,Object> e=new LinkedHashMap<>();e.put("eventId",id);e.put("revision",1);e.put("kind",kind);
  e.put("eventTime","2026-01-01T12:00:00Z");
  for(int i=0;i<fields.length;i+=2)e.put((String)fields[i],fields[i+1]);return e;
 }
 public static List<Map<String,Object>> initial() {
  String a="2026-01-01T12:00:00Z",b="2026-01-01T12:10:00Z";
  return List.of(
   event("spec","SPEC","productCode","DRUG-DEMO","startTime",a,"endTime",b,"minC",2.0,"maxC",8.0,"maxExcursionSeconds",60),
   event("place-A","PLACEMENT","productId","UNIT-A","productCode","DRUG-DEMO","lotId","LOT-1","containerId","BOX-A","startTime",a,"endTime",b),
   event("place-B","PLACEMENT","productId","UNIT-B","productCode","DRUG-DEMO","lotId","LOT-1","containerId","BOX-B","startTime",a,"endTime",b),
   event("assign","ASSIGNMENT","sensorId","SENSOR-1","containerId","BOX-A","startTime",a,"endTime",b),
   event("inventory-A","INVENTORY","productId","UNIT-A","location","WAREHOUSE-1","status","IN_STOCK"),
   event("inventory-B","INVENTORY","productId","UNIT-B","location","HOSPITAL-DEMO","status","DISPATCHED"),
   event("reading-hot","READING","sensorId","SENSOR-1","startTime",a,"endTime",b,"temperatureC",12.0));
 }
 // Declares the producing system so Bronze provenance does not fall back to the topic owner.
 static ProducerRecord<String,String> record(String topic,String key,String value) {
  ProducerRecord<String,String> r=new ProducerRecord<>(topic,key,value);
  r.headers().add(new RecordHeader("source-system","simulator".getBytes(java.nio.charset.StandardCharsets.UTF_8)));return r;
 }
 static String topic(String kind){return switch(kind){case "READING"->"coldchain.sensors";case "SPEC"->"coldchain.specifications";case "INVENTORY"->"coldchain.inventory";default->"coldchain.traceability";};}
 public static void main(String[] args)throws Exception {
  Properties p=new Properties();p.put("bootstrap.servers",System.getenv().getOrDefault("KAFKA_BOOTSTRAP","kafka:9092"));
  p.put("key.serializer",StringSerializer.class.getName());p.put("value.serializer",StringSerializer.class.getName());p.put("acks","all");p.put("enable.idempotence","true");
  try(KafkaProducer<String,String> producer=new KafkaProducer<>(p)) {
   for(Map<String,Object> e:initial())producer.send(record(topic((String)e.get("kind")),(String)e.get("eventId"),JSON.writeValueAsString(e))).get();
   Map<String,Object> reading=initial().get(6);
   producer.send(record("coldchain.sensors","reading-hot",JSON.writeValueAsString(reading))).get();
   producer.send(record("coldchain.sensors","bad","{broken")).get();
   producer.flush();System.out.println("Initial: UNIT-A quality review; UNIT-B insufficient data. Waiting 40s for initial snapshot.");
   Thread.sleep(40000);
   Map<String,Object> correction=new LinkedHashMap<>(initial().get(3));
   correction.put("revision",2);correction.put("containerId","BOX-B");
   producer.send(record("coldchain.traceability","assign",JSON.writeValueAsString(correction))).get();
   System.out.println("Correction sent: sensor was in BOX-B. UNIT-B must become QUALITY_REVIEW; UNIT-A INSUFFICIENT_DATA.");
  }
 }
}

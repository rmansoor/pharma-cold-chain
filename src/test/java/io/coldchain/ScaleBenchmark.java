package io.coldchain;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.apache.spark.sql.*;
// Correction latency at a synthetic scale. Not part of the suite; run with
//   mvn -s maven-settings.xml -B test -Dtest=ScaleBenchmark -Dbench=true [-Dcontainers=200 -Dunits=10 -Dhours=24]
// Each container holds `units` units (half placed directly, half in a carton on a pallet), one calibrated sensor reads every
// 10 minutes for `hours` hours, and one specification applies. The correction moves one sensor to another container.
@EnabledIfSystemProperty(named="bench",matches="true")
class ScaleBenchmark {
 static int prop(String name,int fallback){return Integer.parseInt(System.getProperty(name,String.valueOf(fallback)));}
 static String at(long minutes){return java.time.Instant.parse("2026-01-01T00:00:00Z").plusSeconds(minutes*60).toString();}
 @Test void correctionLatency()throws Exception {
  int containers=prop("containers",200),units=prop("units",10),hours=prop("hours",24);
  SparkSession s=Pipeline.session("cold-chain-benchmark");s.sparkContext().setLogLevel("ERROR");s.conf().set(Specifications.ALLOW_SYNTHETIC,true);
  String end=at(hours*60L),data=Pipeline.DATA;
  List<Map<String,Object>> events=new ArrayList<>();
  events.add(Simulator.event("spec","SPEC","productCode","DRUG-DEMO","startTime",at(0),"endTime",end,"minC",2.0,"maxC",8.0,"maxExcursionSeconds",600,"synthetic",true));
  for(int c=0;c<containers;c++) {
   String box="BOX-"+c,sensor="SENSOR-"+c;
   events.add(Simulator.event("assign-"+c,"ASSIGNMENT","sensorId",sensor,"containerId",box,"startTime",at(0),"endTime",end));
   events.add(Simulator.event("calibration-"+c,"CALIBRATION","sensorId",sensor,"startTime",at(0),"endTime",end,"uncertaintyC",0.5));
   events.add(Simulator.event("custody-"+c,"CUSTODY","containerId",box,"custodianId","CARRIER-1","startTime",at(0),"endTime",end));
   events.add(Simulator.event("pack-pallet-"+c,"PACKING","childId","PALLET-"+c,"childLevel","PALLET","parentId",box,"parentLevel","CONTAINER","startTime",at(0),"endTime",end));
   events.add(Simulator.event("pack-carton-"+c,"PACKING","childId","CARTON-"+c,"childLevel","CARTON","parentId","PALLET-"+c,"parentLevel","PALLET","startTime",at(0),"endTime",end));
   for(int u=0;u<units;u++) {
    String unit="UNIT-"+c+"-"+u;
    events.add(u%2==0?Simulator.event("place-"+unit,"PLACEMENT","productId",unit,"productCode","DRUG-DEMO","lotId","LOT-1","containerId",box,"startTime",at(0),"endTime",end)
     :Simulator.event("pack-"+unit,"PACKING","childId",unit,"childLevel","UNIT","productCode","DRUG-DEMO","lotId","LOT-1","parentId","CARTON-"+c,"parentLevel","CARTON","startTime",at(0),"endTime",end));
    events.add(Simulator.event("inventory-"+unit,"INVENTORY","productId",unit,"location","WAREHOUSE-1","status","IN_STOCK"));
   }
   for(int r=0;r<hours*6;r++)
    events.add(Simulator.event("reading-"+c+"-"+r,"READING","sensorId",sensor,"startTime",at(r*10L),"endTime",at(r*10L+10),"temperatureC",c%10==0&&r%12==0?12.0:5.0));
  }
  String lake=Files.createTempDirectory("cold-chain-bench").toString();
  IncrementalTest.s=s;IncrementalTest.offset=0;
  try {
   Pipeline.DATA=lake;
   long t0=System.nanoTime();Pipeline.process(IncrementalTest.batch(new ArrayList<Object>(events)),0,"bench");long full=(System.nanoTime()-t0)/1_000_000;
   Map<String,Object> move=new LinkedHashMap<>(events.get(1));move.put("revision",2);move.put("containerId","BOX-1");
   long t1=System.nanoTime();Pipeline.process(IncrementalTest.batch(List.<Object>of(move)),1,"bench");long incremental=(System.nanoTime()-t1)/1_000_000;
   Row c=Snapshots.latest(s,lake);
   long t2=System.nanoTime();Row replay=Replay.run(s,lake,lake+"-replay",null);long reference=(System.nanoTime()-t2)/1_000_000;
   for(String d:Replay.DIGESTS)Assertions.assertEquals(c.<Object>getAs(d),replay.<Object>getAs(d),d);
   Assertions.assertEquals(List.of(),Silver.differences(s,lake,Silver.versions(c),lake+"-replay",Silver.versions(replay)),"Silver equals the full replay's");
   System.out.println("BENCH events="+events.size()+" units="+(containers*units)+" containers="+containers+" readings="+(containers*hours*6)
    +" initialFullMs="+full+" correctionIncrementalMs="+incremental+" recomputed="+c.getAs("recomputedProducts")+" fullReplayMs="+reference+" digestsMatch=true silverMatches=true");
  } finally {Pipeline.DATA=data;s.stop();}
 }
}

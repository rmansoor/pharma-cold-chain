package io.coldchain;
import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

// The Kafka-to-Gold correction scenario, scoped to one run. Every identifier (events, units, containers, sensors, product
// code, location) carries the run ID, so a run's expectations can only be met by the records it sent: results retained from
// an earlier run, or another run in the same lake, cannot satisfy them. Each phase is acknowledged by a committed snapshot,
// not by waiting: the newest manifest's Silver must hold every event of the phase at its revision, its quarantine the run's
// malformed record and its conflict cases the run's case, before the phase's expectations are checked against that snapshot.
// Phase 1: duplicates, a payload conflict, a malformed record, nested packaging with a pallet split, a reading exactly at the
// limit and a sampling gap. Phase 2, all late: the sensor assignment corrected, a reading filling the gap, the conflict resolved.
// Load (SID-28 measurements): `load` sends a topology of containers, units and calibrated sensors, then readings at a fixed rate
// for a fixed time (round-robin over sensors, each reading the next 10 minutes; skewed sends every record under one key, so one
// Kafka partition); `await-load` waits for the snapshot holding all of them; `correct` and `await-correction` time one
// assignment correction; `report` summarizes the manifests committed since a time.
// Usage: Scenario send <phase> <run> | Scenario await <phase> <run> [timeout seconds]
//   | Scenario load <run> <containers> <units per container> <readings per second> <seconds> [skewed]
//   | Scenario await-load <run> <records> [timeout] | Scenario correct <run> | Scenario await-correction <run> [timeout]
//   | Scenario report <since epoch ms>
public final class Scenario {
 private Scenario() {}
 record Expect(String unit,String assessment,long excursionSeconds,boolean missingCoverage){}
 static String at(String hhmm){return "2026-01-01T"+hhmm+":00Z";}
 static Map<String,Object> e(String run,String id,String kind,Object... fields) {
  Object[] scoped=fields.clone();
  for(int i=0;i<scoped.length;i+=2)if(scoped[i+1] instanceof String v&&ID_FIELDS.contains(scoped[i]))scoped[i+1]=v+"-"+run;
  return Simulator.event(id+"-"+run,kind,scoped);
 }
 static final Set<String> ID_FIELDS=Set.of("productId","productCode","lotId","containerId","sensorId","childId","parentId","custodianId","location","recipientId","shipmentId");
 static Map<String,Object> revised(Map<String,Object> e,int revision,Object... fields) {
  Map<String,Object> r=new LinkedHashMap<>(e);r.put("revision",revision);
  for(int i=0;i<fields.length;i+=2)r.put((String)fields[i],fields[i+1]);
  return r;
 }
 // The records of a phase, in sending order, as (topic, key, value); a value is JSON, or a malformed string.
 static List<String[]> records(String run,int phase)throws Exception {
  String a=at("12:00"),b=at("12:10"),split=at("12:05");
  Map<String,Object> assign=e(run,"assign-1","ASSIGNMENT","sensorId","SENSOR-1","containerId","BOX-A","startTime",a,"endTime",b);
  Map<String,Object> conflict=e(run,"inventory-C","INVENTORY","productId","UNIT-C","location","WH","status","IN_STOCK");
  List<Map<String,Object>> events=new ArrayList<>();List<String[]> out=new ArrayList<>();
  if(phase==1) {
   events.addAll(List.of(
    e(run,"spec","SPEC","productCode","DRUG","startTime",a,"endTime",b,"minC",2.0,"maxC",8.0,"maxExcursionSeconds",60,"synthetic",true),
    e(run,"place-A","PLACEMENT","productId","UNIT-A","productCode","DRUG","lotId","LOT","containerId","BOX-A","startTime",a,"endTime",b),
    e(run,"place-B","PLACEMENT","productId","UNIT-B","productCode","DRUG","lotId","LOT","containerId","BOX-B","startTime",a,"endTime",b),
    assign,
    e(run,"assign-2","ASSIGNMENT","sensorId","SENSOR-2","containerId","BOX-B","startTime",a,"endTime",b),
    e(run,"calibration-1","CALIBRATION","sensorId","SENSOR-1","startTime",at("00:00"),"endTime",at("23:00"),"uncertaintyC",0.0),
    e(run,"calibration-2","CALIBRATION","sensorId","SENSOR-2","startTime",at("00:00"),"endTime",at("23:00"),"uncertaintyC",0.0),
    e(run,"custody-A","CUSTODY","containerId","BOX-A","custodianId","CARRIER","startTime",a,"endTime",b),
    e(run,"custody-B","CUSTODY","containerId","BOX-B","custodianId","CARRIER","startTime",a,"endTime",b),
    // Out of range for the whole window in BOX-A.
    e(run,"reading-hot","READING","sensorId","SENSOR-1","startTime",a,"endTime",b,"temperatureC",12.0),
    // BOX-B: exactly at the 8.0 °C limit (in range, no uncertainty), then nothing from 12:04 to 12:06, then cool.
    e(run,"reading-limit","READING","sensorId","SENSOR-2","startTime",a,"endTime",at("12:04"),"temperatureC",8.0),
    e(run,"reading-cool","READING","sensorId","SENSOR-2","startTime",at("12:06"),"endTime",b,"temperatureC",5.0),
    // UNIT-D in CARTON-1 on PALLET-1 in BOX-A. UNIT-F in CARTON-2, which leaves PALLET-1 for PALLET-2 (in BOX-B) at 12:05.
    e(run,"pack-D","PACKING","childId","UNIT-D","childLevel","UNIT","productCode","DRUG","lotId","LOT","parentId","CARTON-1","parentLevel","CARTON","startTime",a,"endTime",b),
    e(run,"pack-carton-1","PACKING","childId","CARTON-1","childLevel","CARTON","parentId","PALLET-1","parentLevel","PALLET","startTime",a,"endTime",b),
    e(run,"pack-pallet-1","PACKING","childId","PALLET-1","childLevel","PALLET","parentId","BOX-A","parentLevel","CONTAINER","startTime",a,"endTime",b),
    e(run,"pack-F","PACKING","childId","UNIT-F","childLevel","UNIT","productCode","DRUG","lotId","LOT","parentId","CARTON-2","parentLevel","CARTON","startTime",a,"endTime",b),
    e(run,"pack-carton-2a","PACKING","childId","CARTON-2","childLevel","CARTON","parentId","PALLET-1","parentLevel","PALLET","startTime",a,"endTime",split),
    e(run,"pack-carton-2b","PACKING","childId","CARTON-2","childLevel","CARTON","parentId","PALLET-2","parentLevel","PALLET","startTime",split,"endTime",b),
    e(run,"pack-pallet-2","PACKING","childId","PALLET-2","childLevel","PALLET","parentId","BOX-B","parentLevel","CONTAINER","startTime",a,"endTime",b)));
   for(String u:List.of("A","B","D","F"))events.add(e(run,"inventory-"+u,"INVENTORY","productId","UNIT-"+u,"location","WH","status","IN_STOCK"));
   for(Map<String,Object> x:events)out.add(record(x));
   // A retry of the hot reading at a new offset, a malformed record and two payloads for one ID and revision.
   out.add(record(events.get(9)));
   out.add(new String[]{"coldchain.sensors","bad-"+run,"{broken-"+run});
   out.add(record(conflict));out.add(record(revised(conflict,1,"status","DISPATCHED")));
  } else {
   // Late: the sensor was in BOX-B all along; the gap's reading arrives; a higher revision settles the conflict.
   out.add(record(revised(assign,2,"containerId","BOX-B-"+run)));
   out.add(record(e(run,"reading-gap","READING","sensorId","SENSOR-2","startTime",at("12:04"),"endTime",at("12:06"),"temperatureC",7.0)));
   out.add(record(revised(conflict,2,"status","DISPATCHED")));
  }
  return out;
 }
 static String[] record(Map<String,Object> e)throws Exception{return new String[]{Simulator.topic((String)e.get("kind")),(String)e.get("eventId"),Simulator.JSON.writeValueAsString(e)};}
 // What the phase's snapshot must show for each of the run's units.
 static List<Expect> expected(int phase) {
  return phase==1?List.of(
    new Expect("UNIT-A","QUALITY_REVIEW",600,false),
    new Expect("UNIT-D","QUALITY_REVIEW",600,false),       // through its carton and pallet
    new Expect("UNIT-F","QUALITY_REVIEW",300,true),        // hot in BOX-A until the split, then BOX-B's gap
    new Expect("UNIT-B","INSUFFICIENT_DATA",0,true))       // at the limit is in range; the gap is missing coverage
   :List.of(
    new Expect("UNIT-A","INSUFFICIENT_DATA",0,true),       // its sensor was never in BOX-A
    new Expect("UNIT-D","INSUFFICIENT_DATA",0,true),
    new Expect("UNIT-F","QUALITY_REVIEW",300,true),        // unmeasured in BOX-A, hot in BOX-B after the split
    new Expect("UNIT-B","QUALITY_REVIEW",600,false));      // the corrected sensor reads 12 °C; the gap is filled
 }
 // Event IDs and revisions a snapshot must hold in Silver for phases up to this one (conflicting IDs are withheld until resolved).
 static Map<String,Long> events(String run,int phase)throws Exception {
  Map<String,Long> out=new TreeMap<>();
  for(int p=1;p<=phase;p++)for(String[] r:records(run,p)) {
   if(!r[2].startsWith("{\""))continue;
   Map<?,?> e=Simulator.JSON.readValue(r[2],Map.class);String id=(String)e.get("eventId");
   if(id.startsWith("inventory-C-")&&phase==1)continue;
   out.merge(id,((Number)e.get("revision")).longValue(),Math::max);
  }
  return out;
 }
 // Empty when the newest committed snapshot acknowledges the phase and meets its expectations; otherwise what is missing
 // (acknowledged=false) or wrong.
 record Result(boolean acknowledged,long snapshot,List<String> problems){}
 static Result check(SparkSession s,String base,String run,int phase)throws Exception{return check(s,base,run,phase,expected(phase));}
 static Result check(SparkSession s,String base,String run,int phase,List<Expect> expect)throws Exception {
  Row commit=Snapshots.exists(s,base,Snapshots.COMMITS)?Snapshots.latest(s,base):null;
  if(commit==null)return new Result(false,-1,List.of("no committed snapshot"));
  long snapshot=commit.getAs("snapshotId");
  Map<String,Long> versions=Silver.versions(commit);
  Map<String,Long> want=events(run,phase),have=new HashMap<>();
  Silver.at(s,base,Silver.CURRENT,versions.get(Silver.CURRENT)).filter(col("eventId").endsWith("-"+run)).select("eventId","revision")
   .collectAsList().forEach(r->have.merge(r.getString(0),r.getLong(1),Math::max));
  List<String> missing=new ArrayList<>();
  want.forEach((id,rev)->{if(have.getOrDefault(id,-1L)<rev)missing.add(id+"@"+rev);});
  Dataset<Row> quarantine=Silver.at(s,base,Silver.QUARANTINE,versions.get(Silver.QUARANTINE)).filter(col("raw").equalTo("{broken-"+run));
  if(quarantine.isEmpty())missing.add("quarantined {broken-"+run);
  List<Row> cases=Silver.at(s,base,Silver.CASES,versions.get(Silver.CASES)).filter(col("eventId").equalTo("inventory-C-"+run)).select("status").collectAsList();
  String expectedCase=phase==1?"OPEN":"RESOLVED";
  if(cases.stream().noneMatch(c->expectedCase.equals(c.getString(0))))missing.add("conflict case inventory-C-"+run+" "+expectedCase);
  if(!missing.isEmpty())return new Result(false,snapshot,missing);
  // Acknowledged: the snapshot covers the phase, so its expectations are deterministic from here on.
  List<String> problems=new ArrayList<>();
  if(!quarantine.first().<List<String>>getList(quarantine.first().fieldIndex("reasons")).contains("MALFORMED_JSON"))problems.add("malformed record not MALFORMED_JSON");
  Map<String,Row> units=new HashMap<>();
  Snapshots.assessments(s,base,commit).filter(col("productId").endsWith("-"+run)).collectAsList().forEach(r->units.put(r.getAs("productId"),r));
  for(Expect x:expect) {
   Row r=units.get(x.unit()+"-"+run);
   if(r==null){problems.add(x.unit()+": no assessment");continue;}
   boolean missingCoverage=r.<String>getList(r.fieldIndex("dataQualityFlags")).contains("MISSING_COVERAGE");
   if(!x.assessment().equals(r.getAs("assessment"))||x.excursionSeconds()!=r.<Long>getAs("excursionSeconds")||x.missingCoverage()!=missingCoverage)
    problems.add(x.unit()+": expected "+x.assessment()+" excursion "+x.excursionSeconds()+" s"+(x.missingCoverage()?" with":" without")+" missing coverage, got "
     +r.getAs("assessment")+" excursion "+r.getAs("excursionSeconds")+" s"+(missingCoverage?" with":" without")+" missing coverage (flags "+r.getList(r.fieldIndex("dataQualityFlags"))+")");
  }
  if(units.size()!=expect.size())problems.add("expected "+expect.size()+" units of the run, found "+units.keySet());
  return new Result(true,snapshot,problems);
 }
 static KafkaProducer<String,String> producer() {
  Properties p=new Properties();p.put("bootstrap.servers",System.getenv().getOrDefault("KAFKA_BOOTSTRAP","kafka:9092"));
  p.put("key.serializer",StringSerializer.class.getName());p.put("value.serializer",StringSerializer.class.getName());p.put("acks","all");p.put("enable.idempotence","true");
  p.put("linger.ms","20");p.putAll(KafkaClients.config());
  return new KafkaProducer<>(p);
 }
 // Sends a phase; returns the number of records.
 static int send(String run,int phase)throws Exception {
  List<String[]> records=records(run,phase);
  try(KafkaProducer<String,String> producer=producer()) {
   for(String[] r:records)producer.send(Simulator.record(r[0],r[1],r[2])).get();
  }
  return records.size();
 }
 // The load's topology: per container a calibrated sensor assigned for the day, custody, and units placed in it.
 static List<Map<String,Object>> topology(String run,int containers,int units) {
  List<Map<String,Object>> out=new ArrayList<>();
  out.add(e(run,"spec","SPEC","productCode","DRUG","startTime",at("00:00"),"endTime","2026-01-31T00:00:00Z","minC",2.0,"maxC",8.0,"maxExcursionSeconds",600,"synthetic",true));
  for(int c=0;c<containers;c++) {
   out.add(e(run,"assign-"+c,"ASSIGNMENT","sensorId","SENSOR-"+c,"containerId","BOX-"+c,"startTime",at("00:00"),"endTime","2026-01-31T00:00:00Z"));
   out.add(e(run,"calibration-"+c,"CALIBRATION","sensorId","SENSOR-"+c,"startTime",at("00:00"),"endTime","2026-01-31T00:00:00Z","uncertaintyC",0.5));
   out.add(e(run,"custody-"+c,"CUSTODY","containerId","BOX-"+c,"custodianId","CARRIER","startTime",at("00:00"),"endTime","2026-01-31T00:00:00Z"));
   for(int u=0;u<units;u++)out.add(e(run,"place-"+c+"-"+u,"PLACEMENT","productId","UNIT-"+c+"-"+u,"productCode","DRUG","lotId","LOT","containerId","BOX-"+c,
    "startTime",at("00:00"),"endTime","2026-01-31T00:00:00Z"));
  }
  return out;
 }
 // Sends the topology, then readings at `rate` per second for `seconds`; returns the number of records.
 static long load(String run,int containers,int units,int rate,int seconds,boolean skewed)throws Exception {
  long sent=0;List<java.util.concurrent.Future<RecordMetadata>> pending=new ArrayList<>();
  try(KafkaProducer<String,String> producer=producer()) {
   for(Map<String,Object> x:topology(run,containers,units)) {
    String[] r=record(x);pending.add(producer.send(Simulator.record(r[0],skewed?"hot-"+run:r[1],r[2])));sent++;
   }
   long start=System.currentTimeMillis(),total=(long)rate*seconds;
   for(long i=0;i<total;i++) {
    int c=(int)(i%containers);long n=i/containers;
    java.time.Instant from=java.time.Instant.parse("2026-01-01T00:00:00Z").plusSeconds(n*600);
    Map<String,Object> reading=e(run,"reading-"+c+"-"+n,"READING","sensorId","SENSOR-"+c,"startTime",from.toString(),"endTime",from.plusSeconds(600).toString(),
     "temperatureC",c%10==0&&n%12==0?12.0:5.0);
    String[] r=record(reading);pending.add(producer.send(Simulator.record(r[0],skewed?"hot-"+run:r[1],r[2])));sent++;
    // Paced to the rate: the generator's own schedule, not a synchronization.
    long due=start+(i+1)*1000/rate,wait=due-System.currentTimeMillis();
    if(wait>0)Thread.sleep(wait);
   }
   producer.flush();for(var f:pending)f.get();
  }
  return sent;
 }
 static long runEvents(SparkSession s,String base,Row commit,String run) {
  return Silver.at(s,base,Silver.CURRENT,Silver.versions(commit).get(Silver.CURRENT)).filter(col("eventId").endsWith("-"+run)).count();
 }
 // Polls until done(newest commit) holds; returns the acknowledging commit or null on timeout.
 static Row await(SparkSession s,String base,long timeoutSeconds,java.util.function.Predicate<Row> done)throws Exception {
  long started=System.currentTimeMillis();
  while(System.currentTimeMillis()-started<timeoutSeconds*1000) {
   Row c=Snapshots.exists(s,base,Snapshots.COMMITS)?Snapshots.latest(s,base):null;
   if(c!=null&&done.test(c))return c;
   Thread.sleep(2000);
  }
  return null;
 }
 static long percentile(List<Long> sorted,double p){return sorted.isEmpty()?-1:sorted.get((int)Math.min(sorted.size()-1,Math.ceil(p*sorted.size())-1));}
 // The manifests committed since `since`: snapshots, records, publication lag and batch duration percentiles, throughput.
 static String report(SparkSession s,String base,long since) {
  List<Row> rows=Snapshots.table(s,base,Snapshots.COMMITS).filter(col("committedAt").geq(lit(new java.sql.Timestamp(since))))
   .orderBy("committedAt").select("committedAt","batchRows","publicationLagMs","batchMs").collectAsList();
  if(rows.isEmpty())return "REPORT no snapshots since "+since;
  List<Long> lag=new ArrayList<>(),batch=new ArrayList<>();long records=0;
  for(Row r:rows){records+=r.getLong(1);if(!r.isNullAt(2))lag.add(r.getLong(2));batch.add(r.getLong(3));}
  Collections.sort(lag);Collections.sort(batch);
  double window=(rows.get(rows.size()-1).getTimestamp(0).getTime()-since)/1000.0;
  return String.format(Locale.ROOT,"REPORT snapshots=%d records=%d windowS=%.0f throughputPerS=%.1f lagP50S=%.1f lagP95S=%.1f lagP99S=%.1f lagMaxS=%.1f batchP50S=%.1f batchP95S=%.1f batchMaxS=%.1f",
   rows.size(),records,window,records/window,percentile(lag,.5)/1000.0,percentile(lag,.95)/1000.0,percentile(lag,.99)/1000.0,lag.isEmpty()?-1:lag.get(lag.size()-1)/1000.0,
   percentile(batch,.5)/1000.0,percentile(batch,.95)/1000.0,batch.get(batch.size()-1)/1000.0);
 }
 public static void main(String[] args)throws Exception {
  if(args.length<2)throw new IllegalArgumentException("usage: see Scenario.java");
  switch(args[0]) {
   case "load"->{long t0=System.currentTimeMillis();long n=load(args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]),Integer.parseInt(args[4]),Integer.parseInt(args[5]),args.length>6&&args[6].equals("skewed"));
    System.out.println("SENT load of run "+args[1]+": "+n+" records from "+t0+" to "+System.currentTimeMillis());return;}
   case "correct"->{try(KafkaProducer<String,String> p=producer()){Map<String,Object> a=revised(topology(args[1],1,0).get(1),2,"containerId","BOX-1-"+args[1]);String[] r=record(a);p.send(Simulator.record(r[0],r[1],r[2])).get();}
    System.out.println("SENT correction of run "+args[1]+" at "+System.currentTimeMillis());return;}
   case "await-load","await-correction","report"->{
    SparkSession s=Pipeline.session("Cold-chain scenario");s.sparkContext().setLogLevel("ERROR");long t0=System.currentTimeMillis();
    try {
     if(args[0].equals("report")){System.out.println(report(s,Pipeline.DATA,Long.parseLong(args[1])));return;}
     String run=args[1];
     Row c=args[0].equals("await-load")
      ?await(s,Pipeline.DATA,args.length>3?Long.parseLong(args[3]):3600,x->runEvents(s,Pipeline.DATA,x,run)>=Long.parseLong(args[2]))
      :await(s,Pipeline.DATA,args.length>2?Long.parseLong(args[2]):1800,x->Silver.at(s,Pipeline.DATA,Silver.CURRENT,Silver.versions(x).get(Silver.CURRENT))
        .filter(col("eventId").equalTo("assign-0-"+run).and(col("revision").equalTo(2))).count()>0);
     if(c==null){System.out.println("NOT ACKNOWLEDGED "+args[0]+" of run "+run+" after "+(System.currentTimeMillis()-t0)+" ms");System.exit(2);}
     System.out.println("ACKNOWLEDGED "+args[0]+" of run "+run+" by snapshot "+c.getAs("snapshotId")+" at "+System.currentTimeMillis()+" after "+(System.currentTimeMillis()-t0)
      +" ms, recomputed "+c.getAs("recomputedProducts"));
    } finally {s.stop();}
    return;}
   default->{}
  }
  if(args.length<3)throw new IllegalArgumentException("usage: Scenario send|await <phase> <run> [timeout seconds]");
  int phase=Integer.parseInt(args[1]);String run=args[2];
  if(args[0].equals("send")){System.out.println("SENT phase "+phase+" of run "+run+": "+send(run,phase)+" records");return;}
  long timeout=args.length>3?Long.parseLong(args[3]):600,started=System.currentTimeMillis();
  SparkSession s=Pipeline.session("Cold-chain scenario");s.sparkContext().setLogLevel("ERROR");
  try {
   Result r;
   // Polls only for the acknowledgement; the expectations are checked once, against the snapshot that acknowledged.
   while(!(r=check(s,Pipeline.DATA,run,phase)).acknowledged()&&System.currentTimeMillis()-started<timeout*1000) {
    System.out.println("waiting for a snapshot acknowledging phase "+phase+" of run "+run+" (newest "+r.snapshot()+"): "+r.problems());
    Thread.sleep(5000);
   }
   long ms=System.currentTimeMillis()-started;
   if(!r.acknowledged()){System.out.println("NOT ACKNOWLEDGED phase "+phase+" of run "+run+" after "+ms+" ms: "+r.problems());s.stop();System.exit(2);}
   System.out.println("ACKNOWLEDGED phase "+phase+" of run "+run+" by snapshot "+r.snapshot()+" after "+ms+" ms");
   if(!r.problems().isEmpty()){System.out.println("FAIL phase "+phase+" of run "+run+": "+r.problems());s.stop();System.exit(1);}
   System.out.println("PASS phase "+phase+" of run "+run);
  } finally {s.stop();}
 }
}

package io.coldchain;
import java.util.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

// Replays the Bronze history of one data namespace into another, isolated namespace and recomputes everything in full, as a
// reference for incremental snapshots and for rebuilding Gold under a new checkpoint lineage. The source is only read.
// Usage: Replay <source DATA_DIR> <target DATA_DIR> [bronze version]
public final class Replay {
 private Replay() {}
 // Copies Bronze at `version` (latest when null) into an empty target and publishes snapshot 0 there with a new lineage.
 // Returns the target's manifest.
 public static Row run(SparkSession s,String source,String target,Long version) {
  if(new java.io.File(source).getAbsoluteFile().toPath().normalize().equals(new java.io.File(target).getAbsoluteFile().toPath().normalize()))
   throw new IllegalArgumentException("a replay needs its own namespace, not the source "+source);
  if(Snapshots.exists(s,target,"bronze/events")||Snapshots.exists(s,target,Snapshots.COMMITS))
   throw new IllegalStateException("replay target "+target+" is not empty: a replay namespace is used once");
  long v=version!=null?version:Pipeline.version(s,source+"/bronze/events");
  s.read().format("delta").option("versionAsOf",v).load(source+"/bronze/events").write().format("delta").save(target+"/bronze/events");
  String data=Pipeline.DATA;
  try{Pipeline.DATA=target;Pipeline.publish(s,0,"replay-"+UUID.randomUUID(),true);}finally{Pipeline.DATA=data;}
  return Snapshots.latest(s,target);
 }
 // The source snapshot computed from exactly that Bronze version, if any: the one a replay must reproduce.
 public static Row counterpart(SparkSession s,String source,long bronzeVersion) {
  if(!Snapshots.exists(s,source,Snapshots.COMMITS))return null;
  for(Row c:Snapshots.table(s,source,Snapshots.COMMITS).orderBy(col("snapshotId").desc()).collectAsList()) {
   Long b=Snapshots.recorded(c,"bronze/events");
   if(b!=null&&b==bronzeVersion)return c;
  }
  return null;
 }
 static final List<String> DIGESTS=List.of("products","evidenceRevisions","evidenceDigest","assessmentsDigest","intervalsDigest");
 public static void main(String[] args) {
  if(args.length<2)throw new IllegalArgumentException("usage: Replay <source DATA_DIR> <target DATA_DIR> [bronze version]");
  SparkSession s=Pipeline.session("Cold-chain replay");s.sparkContext().setLogLevel("ERROR");
  long bronze=args.length>2?Long.parseLong(args[2]):Pipeline.version(s,args[0]+"/bronze/events"),started=System.nanoTime();
  // Reproduce the configuration the source snapshot was computed with, not this process's environment.
  Row source=counterpart(s,args[0],bronze);
  if(source!=null&&Arrays.asList(source.schema().fieldNames()).contains("allowSyntheticSpecifications"))
   s.conf().set(Specifications.ALLOW_SYNTHETIC,source.<Boolean>getAs("allowSyntheticSpecifications"));
  Row replayed=run(s,args[0],args[1],bronze);
  System.out.println("REPLAYED "+args[0]+" bronze version "+bronze+" into "+args[1]+" in "+(System.nanoTime()-started)/1_000_000+" ms: "
   +replayed.getAs("products")+" products, lineage "+replayed.getAs("lineageId"));
  Row original=source;
  if(original==null){System.out.println("No source snapshot was computed from that Bronze version; nothing to compare.");s.stop();return;}
  List<String> differ=new ArrayList<>(DIGESTS.stream().filter(d->!Objects.equals(original.getAs(d),replayed.getAs(d))).toList());
  // Silver too, table by table, for snapshots that recorded their Silver versions.
  Map<String,Long> silver=Silver.versions(original);
  differ.addAll(Silver.differences(s,args[0],silver,args[1],Silver.versions(replayed)));
  System.out.println(differ.isEmpty()?"MATCH: source snapshot "+original.getAs("snapshotId")+" equals the full replay (Gold digests and "+silver.size()+" Silver tables)"
   :"MISMATCH: source snapshot "+original.getAs("snapshotId")+" differs in "+differ);
  s.stop();
  if(!differ.isEmpty())System.exit(1);
 }
}

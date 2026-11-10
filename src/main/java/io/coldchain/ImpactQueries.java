package io.coldchain;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

// Investigation queries over one committed snapshot: which units, and which recipients downstream, are affected. Every query
// reads the snapshot's state and the shipments at the versions its manifest recorded, so paging through results is not
// disturbed by snapshots published in between. Every call takes a Scope; there is no unscoped entry point. Recipients are
// organizations or sites; no patient identity is stored or needed.
public final class ImpactQueries {
 private ImpactQueries() {}
 // What a caller may read. An investigator (quality assurance) reads everything. A recipient reads the units shipped to it,
 // with only its own shipments; a site reads the units whose latest known location is that site, without shipments.
 // Authentication is not part of this prototype: the caller's identity and scope must come from the deployment (CC-15).
 public record Scope(String principal,boolean all,Set<String> recipients,Set<String> locations) {
  public static Scope investigator(String principal){return new Scope(principal,true,Set.of(),Set.of());}
  public static Scope recipient(String principal,String... ids){return new Scope(principal,false,Set.of(ids),Set.of());}
  public static Scope site(String principal,String... locations){return new Scope(principal,false,Set.of(),Set.of(locations));}
 }
 // Filters, all optional and combined with AND. lotId matches any of a unit's lots; shipmentId and recipientId match any of
 // its shipments; assessment is an outcome such as QUALITY_REVIEW.
 public record Filter(String productId,String lotId,String shipmentId,String location,String recipientId,String assessment) {
  public static Filter none(){return new Filter(null,null,null,null,null,null);}
 }
 public record Page(long snapshotId,List<Row> rows,String next) {}
 public static final class AccessDenied extends RuntimeException {AccessDenied(String m){super(m);}}

 static void authorize(Scope scope,Filter f) {
  if(scope.all())return;
  if(f.recipientId()!=null&&!scope.recipients().contains(f.recipientId()))
   throw new AccessDenied(scope.principal()+" may not read recipient "+f.recipientId());
  if(f.location()!=null&&!scope.locations().contains(f.location())&&scope.recipients().isEmpty())
   throw new AccessDenied(scope.principal()+" may not read location "+f.location());
 }
 static String fingerprint(Scope scope,Filter f) {
  String text=scope.principal()+"|"+scope.all()+"|"+new TreeSet<>(scope.recipients())+"|"+new TreeSet<>(scope.locations())+"|"+f;
  try {
   return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))).substring(0,16);
  } catch(java.security.NoSuchAlgorithmException e){throw new IllegalStateException(e);}
 }
 // The cursor names the snapshot, the last unit returned and the query it belongs to. It carries no authority: the scope is
 // checked again on every call, and a cursor from another query or scope is refused.
 static String cursor(long snapshot,String last,String query) {
  return Base64.getUrlEncoder().withoutPadding().encodeToString(("v1|"+snapshot+"|"+query+"|"+last).getBytes(StandardCharsets.UTF_8));
 }
 record Position(long snapshot,String query,String last) {}
 static Position position(String cursor) {
  String[] p=new String(Base64.getUrlDecoder().decode(cursor),StandardCharsets.UTF_8).split("\\|",4);
  if(p.length!=4||!p[0].equals("v1"))throw new IllegalArgumentException("not a query cursor");
  return new Position(Long.parseLong(p[1]),p[2],p[3]);
 }
 static Row commit(SparkSession s,String base,long snapshot) {
  if(!Snapshots.exists(s,base,Snapshots.COMMITS))throw new IllegalArgumentException("no committed snapshot");
  List<Row> c=Snapshots.table(s,base,Snapshots.COMMITS).filter(col("snapshotId").equalTo(snapshot)).collectAsList();
  if(c.isEmpty())throw new IllegalArgumentException("snapshot "+snapshot+" is not committed");
  return c.get(0);
 }
 // Each unit of the snapshot with its full assessment evidence, the snapshot's dataAsOf and its shipments.
 static Dataset<Row> units(SparkSession s,String base,Row commit) {
  Dataset<Row> state=Snapshots.assessments(s,base,commit)
   .withColumn("inventoryStale",array_contains(col("dataQualityFlags"),"STALE_INVENTORY"))
   .withColumn("dataAsOf",lit(commit.<java.sql.Timestamp>getAs("dataAsOf")));
  Long version=Snapshots.recorded(commit,"silver/shipments");
  if(version==null)return state.withColumn("shipments",lit(null).cast("array<struct<shipmentId:string,recipientId:string,dispatchedAt:timestamp,deliveredAt:timestamp,eventId:string,revision:bigint>>"));
  Dataset<Row> lines=s.read().format("delta").option("versionAsOf",version).load(base+"/silver/shipments")
   .groupBy(col("productId").as("unit")).agg(sort_array(collect_set(struct(col("shipmentId"),col("recipientId"),col("dispatchedAt"),
    col("deliveredAt"),col("eventId"),col("revision")))).alias("shipments"));
  return state.join(lines,state.col("productId").equalTo(lines.col("unit")),"left").drop("unit");
 }
 // Units a limited scope may read: shipped to one of its recipients, or last known at one of its locations.
 static Dataset<Row> visible(Dataset<Row> units,Scope scope) {
  // A unit without shipments gives null here, which the filter treats as false.
  Column shipped=exists(col("shipments"),x->x.getField("recipientId").isin(scope.recipients().toArray()));
  return units.filter(shipped.or(col("location").isin(scope.locations().toArray())));
 }
 // One page of affected units, ordered by productId. Pass cursor null for the first page: it pins the newest committed
 // snapshot. Later pages, given the returned cursor, read that same snapshot whatever has been published since.
 public static Page query(SparkSession s,String base,Scope scope,Filter f,int limit,String cursor) {
  if(limit<1)throw new IllegalArgumentException("limit must be positive");
  authorize(scope,f);
  String query=fingerprint(scope,f);
  Position at=cursor==null?null:position(cursor);
  if(at!=null&&!at.query().equals(query))throw new IllegalArgumentException("cursor belongs to a different query or scope");
  Row commit=at==null?Snapshots.latest(s,base):commit(s,base,at.snapshot());
  if(commit==null)return new Page(-1,List.of(),null);
  long snapshot=commit.getAs("snapshotId");
  Dataset<Row> rows=units(s,base,commit);
  // Scope first: a recipient sees only its own shipments, a site sees none.
  if(!scope.all())rows=visible(rows.withColumn("shipments",
   functions.filter(col("shipments"),x->x.getField("recipientId").isin(scope.recipients().toArray()))),scope);
  if(f.productId()!=null)rows=rows.filter(col("productId").equalTo(f.productId()));
  if(f.lotId()!=null)rows=rows.filter(array_contains(col("lotIds"),f.lotId()));
  if(f.location()!=null)rows=rows.filter(col("location").equalTo(f.location()));
  if(f.assessment()!=null)rows=rows.filter(col("assessment").equalTo(f.assessment()));
  if(f.shipmentId()!=null)rows=rows.filter(array_contains(col("shipments.shipmentId"),f.shipmentId()));
  if(f.recipientId()!=null)rows=rows.filter(array_contains(col("shipments.recipientId"),f.recipientId()));
  if(at!=null)rows=rows.filter(col("productId").gt(at.last()));
  List<Row> page=rows.orderBy("productId").limit(limit+1).collectAsList();
  boolean more=page.size()>limit;
  if(more)page=page.subList(0,limit);
  return new Page(snapshot,page,more?cursor(snapshot,page.get(page.size()-1).getAs("productId"),query):null);
 }
 // The interval evidence behind one unit's assessment in a committed snapshot, if the scope may read that unit.
 public static Dataset<Row> evidence(SparkSession s,String base,Scope scope,long snapshot,String productId) {
  Row commit=commit(s,base,snapshot);
  // Visibility is judged in the requested snapshot, not the newest one.
  if(!scope.all()&&visible(units(s,base,commit).filter(col("productId").equalTo(productId)),scope).isEmpty())
   throw new AccessDenied(scope.principal()+" may not read the evidence of "+productId);
  return Snapshots.intervals(s,base,commit).filter(col("productId").equalTo(productId));
 }
}

package io.coldchain;
import java.util.*;
import org.apache.spark.sql.*;

// Command-line investigation query over the newest committed snapshot (or the one a cursor names). Prints one JSON line per
// unit, then the cursor for the next page, if any. The caller is COLDCHAIN_PRINCIPAL; the scope must be granted to it.
// Usage: Query --scope investigator|recipient:<id>|site:<location> [--product id] [--lot id]
//   [--shipment id] [--location id] [--recipient id] [--assessment outcome] [--limit n] [--cursor c]
public final class Query {
 private Query() {}
 static final List<String> FIELDS=List.of("productId","lotIds","productCodes","assessment","assessmentReasons","dataQualityFlags",
  "excursionSeconds","allowedExcursionSeconds","coveredSeconds","expectedSeconds","location","inventoryStatus","inventoryRecordedAt",
  "inventoryAgeSeconds","inventoryStale","shipments","sourceRevisions","snapshotId","dataAsOf","policyVersion","ruleVersion","assessmentHash");
 public static void main(String[] args) {
  Map<String,String> a=new HashMap<>();
  for(int i=0;i+1<args.length;i+=2)a.put(args[i].replaceFirst("^--",""),args[i+1]);
  String scope=a.getOrDefault("scope","");
  if(!scope.matches("investigator|recipient:.+|site:.+"))throw new IllegalArgumentException("--scope investigator|recipient:<id>|site:<location> is required");
  ImpactQueries.Filter f=new ImpactQueries.Filter(a.get("product"),a.get("lot"),a.get("shipment"),a.get("location"),a.get("recipient"),a.get("assessment"));
  SparkSession spark=Pipeline.session("Cold-chain query");spark.sparkContext().setLogLevel("ERROR");
  try {
   // The caller (COLDCHAIN_PRINCIPAL) may use only a scope its registry entry grants.
   ImpactQueries.Scope s=Governance.scope(spark,Governance.caller(),scope);
   ImpactQueries.Page page=ImpactQueries.query(spark,Pipeline.DATA,s,f,Integer.parseInt(a.getOrDefault("limit","50")),a.get("cursor"));
   for(Row r:page.rows())System.out.println(spark.createDataFrame(List.of(r),r.schema()).selectExpr(FIELDS.toArray(String[]::new)).toJSON().first());
   System.out.println("SNAPSHOT "+page.snapshotId()+" ROWS "+page.rows().size()+(page.next()==null?"":" NEXT "+page.next()));
  } catch(ImpactQueries.AccessDenied e){System.out.println("DENIED: "+e.getMessage());spark.stop();System.exit(3);}
  finally {spark.stop();}
 }
}

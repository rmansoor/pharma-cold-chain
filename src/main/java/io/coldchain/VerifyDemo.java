package io.coldchain;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
public final class VerifyDemo {
 public static void main(String[] args)throws Exception {
  SparkSession s=Pipeline.session("Cold-chain verifier");long end=System.nanoTime()+180_000_000_000L;
  String base=Pipeline.DATA;
  while(System.nanoTime()<end) {
   if(DeltaTable.isDeltaTable(s,base+"/gold/commits")) {
    long latest=s.read().format("delta").load(base+"/gold/commits").agg(max("snapshotId")).first().getLong(0);
    Dataset<Row> all=s.read().format("delta").load(base+"/gold/assessment_history");
    Dataset<Row> now=all.filter(col("snapshotId").equalTo(latest));
    boolean before=all.filter("productId='UNIT-A' AND assessment='QUALITY_REVIEW'").count()>0;
    boolean b=now.filter("productId='UNIT-B' AND assessment='QUALITY_REVIEW' AND excursionSeconds=600 AND coveredSeconds=600").count()==1;
    boolean a=now.filter("productId='UNIT-A' AND assessment='INSUFFICIENT_DATA' AND coveredSeconds=0").count()==1;
    boolean dlq=DeltaTable.isDeltaTable(s,base+"/silver/quarantine") && s.read().format("delta").load(base+"/silver/quarantine").filter("raw='{broken'").count()>0;
    if(before&&a&&b&&dlq) {
     now.orderBy("productId").show(false);
     System.out.println("PASS: revised assignment moved exposure A -> B; prior assessment retained; duplicate did not inflate duration; malformed input quarantined.");s.stop();return;
    }
   }
   Thread.sleep(3000);
  }
  s.stop();throw new AssertionError("Scenario incomplete. Use a fresh deployment and allow the initial phase to commit before correction.");
 }
}

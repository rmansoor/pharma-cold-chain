package io.coldchain;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
// Prints one line of persisted-state counts so restart checks can compare before and after.
public final class Inspect {
 public static void main(String[] args) {
  SparkSession s=Pipeline.session("Cold-chain inspect");s.sparkContext().setLogLevel("ERROR");String base=Pipeline.DATA;
  if(!DeltaTable.isDeltaTable(s,base+"/gold/commits")){System.out.println("STATE empty");s.stop();return;}
  Dataset<Row> bronze=s.read().format("delta").load(base+"/bronze/events");
  Row c=s.read().format("delta").load(base+"/gold/commits").agg(max("snapshotId"),count(lit(1))).first();
  System.out.println("STATE bronze="+bronze.count()+" bronzeKeys="+bronze.select("topic","partition","offset").distinct().count()
   +" history="+s.read().format("delta").load(base+"/gold/assessment_history").count()
   +" commits="+c.getLong(1)+" lastSnapshot="+c.getLong(0)
   +" quarantine="+s.read().format("delta").load(base+"/silver/quarantine").count());
  s.stop();
 }
}

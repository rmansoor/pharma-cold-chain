package io.coldchain;
import java.util.Arrays;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
// Prints one line of persisted-state counts so restart checks can compare before and after.
public final class Inspect {
 public static void main(String[] args) {
  SparkSession s=Pipeline.session("Cold-chain inspect");s.sparkContext().setLogLevel("ERROR");String base=Pipeline.DATA;
  if(!DeltaTable.isDeltaTable(s,base+"/bronze/events")){System.out.println("STATE empty");s.stop();return;}
  Dataset<Row> bronze=s.read().format("delta").load(base+"/bronze/events");
  // Before the first Gold commit (for example after an injected crash) only Bronze exists.
  boolean gold=DeltaTable.isDeltaTable(s,base+"/gold/commits");
  Row c=gold?s.read().format("delta").load(base+"/gold/commits").agg(max("snapshotId"),count(lit(1))).first():null;
  System.out.println("STATE bronze="+bronze.count()+" bronzeKeys="+bronze.select("topic","partition","offset").distinct().count()
   +" declared="+(Arrays.asList(bronze.columns()).contains("sourceSystemDeclared")?bronze.filter("sourceSystemDeclared").count():0)
   +" history="+(gold?s.read().format("delta").load(base+"/gold/assessment_history").count():0)
   +" commits="+(gold?c.getLong(1):0)+" lastSnapshot="+(gold?c.getLong(0):-1)
   +" quarantine="+(gold?s.read().format("delta").load(base+"/silver/quarantine").count():0));
  s.stop();
 }
}

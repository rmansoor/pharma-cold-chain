package io.coldchain;
import java.util.Arrays;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
// Prints one line of persisted-state counts so restart checks can compare before and after.
public final class Inspect {
 public static void main(String[] args) {
  SparkSession s=Pipeline.session("Cold-chain inspect");s.sparkContext().setLogLevel("ERROR");String base=Pipeline.DATA;
  if(!Snapshots.exists(s,base,"bronze/events")){System.out.println("STATE empty");s.stop();return;}
  Dataset<Row> bronze=s.read().format("delta").load(base+"/bronze/events");
  // Before the first Gold commit (for example after an injected crash) only Bronze exists.
  boolean gold=Snapshots.exists(s,base,"gold/commits");
  Row c=gold?s.read().format("delta").load(base+"/gold/commits").agg(max("snapshotId"),count(lit(1))).first():null;
  System.out.println("STATE bronze="+bronze.count()+" bronzeKeys="+bronze.select("topic","partition","offset").distinct().count()
   +" declared="+(Arrays.asList(bronze.columns()).contains("sourceSystemDeclared")?bronze.filter("sourceSystemDeclared").count():0)
   +" history="+(gold?s.read().format("delta").load(base+"/gold/assessment_history").count():0)
   +" commits="+(gold?c.getLong(1):0)+" lastSnapshot="+(gold?c.getLong(0):-1)
   +" quarantine="+(gold?s.read().format("delta").load(base+"/silver/quarantine").count():0)
   +" caseLog="+(Snapshots.exists(s,base,"silver/case_log")?s.read().format("delta").load(base+"/silver/case_log").count():0)
   // visible: rows a reader of the pinned snapshot sees; uncommitted: history rows of snapshots without a manifest.
   +" visible="+(gold?Snapshots.assessments(s,base,Snapshots.latest(s,base)).count():0)
   +" uncommitted="+(Snapshots.exists(s,base,"gold/assessment_history")?Snapshots.uncommitted(s,base,Snapshots.HISTORY).count():0)
   // outbox: committed review items; relayed: items the relay recorded as sent; pending: committed snapshots not yet relayed.
   +" outbox="+(gold&&Snapshots.exists(s,base,Outbox.OUTBOX)?Snapshots.table(s,base,Outbox.OUTBOX).join(Snapshots.table(s,base,Snapshots.COMMITS).select("snapshotId"),"snapshotId","left_semi").count():0)
   +" relayed="+(Snapshots.exists(s,base,Outbox.RELAYED)?Snapshots.table(s,base,Outbox.RELAYED).agg(coalesce(sum("items"),lit(0L))).first().getLong(0):0)
   +" pending="+Outbox.pending(s,base).size()
   // The case store (CASES_DIR): notifications received, distinct deliveries among them, and open cases.
   +" notifications="+(Snapshots.exists(s,CaseStore.DIR,CaseStore.NOTIFICATIONS)?Snapshots.table(s,CaseStore.DIR,CaseStore.NOTIFICATIONS).count():0)
   +" deliveries="+(Snapshots.exists(s,CaseStore.DIR,CaseStore.NOTIFICATIONS)?Snapshots.table(s,CaseStore.DIR,CaseStore.NOTIFICATIONS).select("deliveryId").distinct().count():0)
   +" openCases="+(Snapshots.exists(s,CaseStore.DIR,CaseStore.CASES)?Snapshots.table(s,CaseStore.DIR,CaseStore.CASES).filter("status = 'OPEN'").count():0));
  s.stop();
 }
}

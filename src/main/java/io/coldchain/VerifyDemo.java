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
    // UNIT-D follows BOX-A through CARTON-1 and PALLET-1, so it moves exactly like UNIT-A. Evidence IDs come from
    // matched segments, so the three lineage events appear on the reviewed assessment, not on the uncovered one.
    boolean nested=all.filter("productId='UNIT-D' AND assessment='QUALITY_REVIEW' AND size(lineageIds)=3").count()>0
     && now.filter("productId='UNIT-D' AND assessment='INSUFFICIENT_DATA' AND coveredSeconds=0").count()==1;
    boolean dlq=DeltaTable.isDeltaTable(s,base+"/silver/quarantine") && s.read().format("delta").load(base+"/silver/quarantine")
     .filter("raw='{broken' AND reasons=array('MALFORMED_JSON') AND status='OPEN'").count()>0;
    // The inventory-C conflict was opened by the initial phase and resolved by a later snapshot.
    boolean resolved=DeltaTable.isDeltaTable(s,base+"/silver/case_log") && s.read().format("delta").load(base+"/silver/case_log")
     .filter("eventId='inventory-C' AND caseType='PAYLOAD_MISMATCH'").groupBy().pivot("status",java.util.List.of("OPEN","RESOLVED")).agg(min("snapshotId"))
     .filter("OPEN < RESOLVED").count()==1;
    // The assignment correction lists the units it took exposure from (BOX-A) and gave it to (BOX-B).
    boolean invalidated=DeltaTable.isDeltaTable(s,base+"/silver/mapping_invalidations") && s.read().format("delta").load(base+"/silver/mapping_invalidations")
     .filter("assignmentId='assign'").selectExpr("concat(side,':',productId) v").distinct().orderBy("v").as(Encoders.STRING()).collectAsList()
     .equals(java.util.List.of("FORMER:UNIT-A","FORMER:UNIT-D","REVISED:UNIT-B"));
    // A handover at 12:05 leaves no custody gap in BOX-A; BOX-B has none recorded.
    boolean custody=now.filter("productId='UNIT-A' AND missingCustodySeconds=0 AND size(custodianIds)=2").count()==1
     && now.filter("productId='UNIT-B' AND missingCustodySeconds=600").count()==1;
    // The demo's thresholds are synthetic and flagged as such; SENSOR-1's calibration covers the hot reading.
    boolean quality=now.filter("productId='UNIT-B' AND syntheticThresholds AND calibrationIds=array('calibration-1') AND uncalibratedSeconds=0 "
     +"AND assessmentReasons[0]='EXCURSION_ABOVE_ALLOWANCE' AND ruleVersion='exposure-v4'").count()==1
     && DeltaTable.isDeltaTable(s,base+"/silver/readings") && s.read().format("delta").load(base+"/silver/readings")
     .filter("eventId='reading-hot' AND calibration='CALIBRATED' AND uncertaintyC=0.5").count()==1
     && s.read().format("delta").load(base+"/silver/specifications").filter("specificationId='spec' AND synthetic").count()==1;
    if(before&&a&&b&&nested&&dlq&&resolved&&invalidated&&custody&&quality) {
     now.orderBy("productId").show(false);
     System.out.println("PASS: revised assignment moved exposure A -> B; prior assessment retained; nested packaging followed; correction invalidated former and revised units; custody handover without gap; synthetic thresholds flagged and calibrated reading used; duplicate did not inflate duration; malformed input quarantined with a reason code; conflict case opened and resolved by a higher revision.");s.stop();return;
    }
   }
   Thread.sleep(3000);
  }
  s.stop();throw new AssertionError("Scenario incomplete. Use a fresh deployment and allow the initial phase to commit before correction.");
 }
}

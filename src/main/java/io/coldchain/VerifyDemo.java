package io.coldchain;
import java.util.*;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
public final class VerifyDemo {
 public static void main(String[] args)throws Exception {
  // Batches on a shared CI runner can take minutes each; VERIFY_TIMEOUT_SECONDS bounds the wait for the corrected snapshot.
  long timeout=Long.parseLong(System.getenv().getOrDefault("VERIFY_TIMEOUT_SECONDS","600"));
  SparkSession s=Pipeline.session("Cold-chain verifier");long end=System.nanoTime()+timeout*1_000_000_000L;
  String failing="no committed snapshot";
  String base=Pipeline.DATA;
  while(System.nanoTime()<end) {
   if(Snapshots.exists(s,base,"gold/commits")) {
    // Pin one committed snapshot and read everything at it; uncommitted rows are never read.
    Row commit=Snapshots.latest(s,base);
    if(commit==null){Thread.sleep(3000);continue;}
    long latest=commit.getAs("snapshotId");
    Dataset<Row> all=Snapshots.committedHistory(s,base);
    Dataset<Row> now=Snapshots.assessments(s,base,commit);
    boolean before=all.filter("productId='UNIT-A' AND assessment='QUALITY_REVIEW'").count()>0;
    boolean b=now.filter("productId='UNIT-B' AND assessment='QUALITY_REVIEW' AND excursionSeconds=600 AND coveredSeconds=600").count()==1;
    boolean a=now.filter("productId='UNIT-A' AND assessment='INSUFFICIENT_DATA' AND coveredSeconds=0").count()==1;
    // UNIT-D follows BOX-A through CARTON-1 and PALLET-1, so it moves exactly like UNIT-A. Evidence IDs come from
    // matched segments, so the three lineage events appear on the reviewed assessment, not on the uncovered one.
    boolean nested=all.filter("productId='UNIT-D' AND assessment='QUALITY_REVIEW' AND size(lineageIds)=3").count()>0
     && now.filter("productId='UNIT-D' AND assessment='INSUFFICIENT_DATA' AND coveredSeconds=0").count()==1;
    boolean dlq=Snapshots.exists(s,base,"silver/quarantine") && s.read().format("delta").load(base+"/silver/quarantine")
     .filter("raw='{broken' AND reasons=array('MALFORMED_JSON') AND status='OPEN'").count()>0;
    // The inventory-C conflict was opened by the initial phase and resolved by a later snapshot.
    boolean resolved=Snapshots.exists(s,base,"silver/case_log") && s.read().format("delta").load(base+"/silver/case_log")
     .filter("eventId='inventory-C' AND caseType='PAYLOAD_MISMATCH'").groupBy().pivot("status",java.util.List.of("OPEN","RESOLVED")).agg(min("snapshotId"))
     .filter("OPEN < RESOLVED").count()==1;
    // The assignment correction lists the units it took exposure from (BOX-A) and gave it to (BOX-B).
    boolean invalidated=Snapshots.exists(s,base,"silver/mapping_invalidations") && s.read().format("delta").load(base+"/silver/mapping_invalidations")
     .filter("assignmentId='assign'").selectExpr("concat(side,':',productId) v").distinct().orderBy("v").as(Encoders.STRING()).collectAsList()
     .equals(java.util.List.of("FORMER:UNIT-A","FORMER:UNIT-D","REVISED:UNIT-B"));
    // A handover at 12:05 leaves no custody gap in BOX-A; BOX-B has none recorded.
    boolean custody=now.filter("productId='UNIT-A' AND missingCustodySeconds=0 AND size(custodianIds)=2").count()==1
     && now.filter("productId='UNIT-B' AND missingCustodySeconds=600").count()==1;
    // The demo's thresholds are synthetic and flagged as such; SENSOR-1's calibration covers the hot reading.
    boolean quality=now.filter("productId='UNIT-B' AND syntheticThresholds AND calibrationIds=array('calibration-1') AND uncalibratedSeconds=0 "
     +"AND assessmentReasons[0]='EXCURSION_ABOVE_ALLOWANCE' AND ruleVersion='exposure-v4'").count()==1
     && Snapshots.exists(s,base,"silver/readings") && s.read().format("delta").load(base+"/silver/readings")
     .filter("eventId='reading-hot' AND calibration='CALIBRATED' AND uncertaintyC=0.5").count()==1
     && s.read().format("delta").load(base+"/silver/specifications").filter("specificationId='spec' AND synthetic").count()==1;
    // Interval evidence names the exact revision: UNIT-B's excursion now rests on assignment revision 2, and the earlier
    // snapshot's UNIT-A excursion, on revision 1, is still retrievable.
    boolean traced=Snapshots.exists(s,base,"gold/exposure_intervals");
    if(traced) {
     Dataset<Row> intervals=s.read().format("delta").load(base+"/gold/exposure_intervals").join(s.read().format("delta").load(base+"/gold/commits").select("snapshotId"),"snapshotId","left_semi");
     String assign="exists(sources,x -> x.kind='ASSIGNMENT' AND x.eventId='assign' AND x.revision=%d)";
     // The pinned snapshot's intervals: each unit's come from the snapshot that last recomputed it.
     Dataset<Row> pinned=Snapshots.intervals(s,base,commit);
     traced=pinned.filter("productId='UNIT-B' AND metric='EXCURSION' AND seconds=600 AND "+assign.formatted(2)).count()==1
      && pinned.filter("productId='UNIT-A' AND metric='MISSING' AND seconds=600").count()==1
      && intervals.filter("productId='UNIT-A' AND metric='EXCURSION' AND "+assign.formatted(1)).count()>0;
    }
    // Outcomes name a persisted policy that releases nothing, and flags are reported beside them whatever the outcome.
    boolean policy=Snapshots.exists(s,base,"gold/assessment_policies") && s.read().format("delta").load(base+"/gold/assessment_policies")
     .filter("policyVersion='"+Policy.VERSION+"' AND size(states)=3 AND forall(states,x -> NOT x.impliesRelease)").count()==1
     && now.filter("policyVersion='"+Policy.VERSION+"'").count()==now.count()
     && now.filter("productId='UNIT-B' AND assessment='QUALITY_REVIEW' AND dataQualityFlags=array('MISSING_CUSTODY')").count()==1
     && now.filter("productId='UNIT-D' AND dataQualityFlags=array('MISSING_COVERAGE','UNKNOWN_INVENTORY')").count()==1;
    // The manifest names the streaming lineage, this run, source high-watermarks and the table versions it read and wrote,
    // and its evidence digest matches the pinned assessments.
    String digest=now.selectExpr("explode(sourceRevisions) x").selectExpr("concat_ws(':',x.kind,x.eventId,x.revision,x.payloadHash) r")
     .agg(sha2(array_join(array_sort(collect_set("r")),"\n"),256)).first().getString(0);
    boolean published=!"unattached".equals(commit.getAs("lineageId")) && commit.getAs("runId")!=null
     && commit.<Long>getAs("products")==now.count() && digest.equals(commit.getAs("evidenceDigest"))
     && commit.getList(commit.fieldIndex("sourceHighWatermarks")).size()>0
     && Snapshots.recorded(commit,"bronze/events")!=null && Snapshots.recorded(commit,"gold/assessment_history")!=null
     // The correction was applied incrementally, not by recomputing everything.
     && "INCREMENTAL".equals(commit.getAs("recomputeMode"));
    // Investigation: the reviewed unit went to HOSPITAL-DEMO in SHP-DEMO. The hospital sees only its own unit and may not ask
    // about another recipient; paging the lot two units at a time stays on one snapshot.
    boolean impact;
    try {
     ImpactQueries.Filter toHospital=new ImpactQueries.Filter(null,null,null,null,"HOSPITAL-DEMO",null);
     ImpactQueries.Page shipped=ImpactQueries.query(s,base,ImpactQueries.Scope.investigator("verifier"),toHospital,10,null);
     ImpactQueries.Page own=ImpactQueries.query(s,base,ImpactQueries.Scope.recipient("hospital","HOSPITAL-DEMO"),ImpactQueries.Filter.none(),10,null);
     ImpactQueries.Filter lot=new ImpactQueries.Filter(null,"LOT-1",null,null,null,null);
     ImpactQueries.Page first=ImpactQueries.query(s,base,ImpactQueries.Scope.investigator("verifier"),lot,2,null);
     ImpactQueries.Page second=first.next()==null?null:ImpactQueries.query(s,base,ImpactQueries.Scope.investigator("verifier"),lot,2,first.next());
     boolean denied=false;
     try{ImpactQueries.query(s,base,ImpactQueries.Scope.recipient("hospital","HOSPITAL-DEMO"),new ImpactQueries.Filter(null,null,null,null,"HOSPITAL-2",null),10,null);}
     catch(ImpactQueries.AccessDenied e){denied=true;}
     impact=shipped.rows().size()==1&&"UNIT-B".equals(shipped.rows().get(0).getAs("productId"))&&"QUALITY_REVIEW".equals(shipped.rows().get(0).getAs("assessment"))
      &&own.rows().size()==1&&denied&&second!=null&&second.snapshotId()==first.snapshotId()&&first.rows().size()+second.rows().size()==3&&second.next()==null;
    } catch(IllegalArgumentException e){impact=false;}
    // Review items reached the reviewers' case store once each: every committed item was delivered, no delivery was recorded
    // twice, each case holds its unit's newest item, UNIT-B's case is open for review, and UNIT-A's first item was revised.
    boolean alerts=false;
    if(Snapshots.exists(s,base,Outbox.OUTBOX)&&Snapshots.exists(s,CaseStore.DIR,CaseStore.CASES)) {
     Dataset<Row> items=Snapshots.table(s,base,Outbox.OUTBOX).join(Snapshots.table(s,base,"gold/commits").select("snapshotId"),"snapshotId","left_semi");
     Dataset<Row> told=Snapshots.table(s,CaseStore.DIR,CaseStore.NOTIFICATIONS),cases=Snapshots.table(s,CaseStore.DIR,CaseStore.CASES);
     Dataset<Row> newest=items.groupBy("reviewItemId").agg(max("snapshotId").alias("snapshotId"));
     alerts=items.select("deliveryId").except(told.select("deliveryId")).isEmpty()&&told.count()==told.select("deliveryId").distinct().count()
      &&newest.join(cases,new String[]{"reviewItemId","snapshotId"},"left_anti").isEmpty()&&cases.count()==newest.count()
      &&cases.filter("reviewItemId='UNIT-B' AND status='OPEN' AND assessment='QUALITY_REVIEW' AND size(sourceRevisions) > 0").count()==1
      &&told.filter("productId='UNIT-A'").orderBy("snapshotId").selectExpr("concat(change,':',assessment) c").as(Encoders.STRING()).collectAsList()
       .equals(List.of("NEW:QUALITY_REVIEW","REVISED:INSUFFICIENT_DATA"));
    }
    // Name the checks that fail, whenever that set changes, so a timeout says what never became true.
    Map<String,Boolean> checks=new LinkedHashMap<>();
    checks.put("before",before);checks.put("a",a);checks.put("b",b);checks.put("nested",nested);checks.put("dlq",dlq);checks.put("resolved",resolved);
    checks.put("invalidated",invalidated);checks.put("custody",custody);checks.put("quality",quality);checks.put("traced",traced);
    checks.put("policy",policy);checks.put("published",published);checks.put("impact",impact);checks.put("alerts",alerts);
    String now_failing="snapshot "+latest+": "+String.join(",",checks.entrySet().stream().filter(e->!e.getValue()).map(Map.Entry::getKey).toList());
    if(!now_failing.equals(failing))System.out.println("waiting on "+now_failing);
    failing=now_failing;
    if(!checks.containsValue(false)) {
     now.orderBy("productId").show(false);
     System.out.println("Pinned snapshot "+latest+" ("+commit.getAs("recomputeMode")+", "+commit.getAs("recomputedProducts")+" units recomputed for "
      +commit.getAs("changedEvents")+" changed events): lineage "+commit.getAs("lineageId")+", run "+commit.getAs("runId")+", code "+commit.getAs("codeVersion")
      +", evidence "+commit.getAs("evidenceRevisions")+" revisions, tables "+commit.getList(commit.fieldIndex("tableVersions")));
     System.out.println("PASS: revised assignment moved exposure A -> B; prior assessment retained; nested packaging followed; correction invalidated former and revised units; custody handover without gap; synthetic thresholds flagged and calibrated reading used; exposure intervals traced to exact revisions; outcomes name a policy that releases nothing, with data-quality flags beside them; duplicate did not inflate duration; malformed input quarantined with a reason code; conflict case opened and resolved by a higher revision; the reviewed unit traced to its recipient within scope; review items delivered to the case store once each, newest revision held; manifest identifies lineage, run, high-watermarks, table versions and evidence.");s.stop();return;
    }
   }
   Thread.sleep(3000);
  }
  s.stop();throw new AssertionError("Scenario incomplete after "+timeout+" s ("+failing+"). Use a fresh deployment and allow the initial phase to commit before correction.");
 }
}

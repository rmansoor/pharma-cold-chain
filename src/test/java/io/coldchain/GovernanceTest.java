package io.coldchain;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
// Access control, audit, quality approval of rule changes and retention, against lakes built by the real pipeline.
class GovernanceTest {
 static SparkSession s;static String data,principals;
 @BeforeAll static void start()throws Exception {
  s=Pipeline.session("cold-chain-governance-tests");s.sparkContext().setLogLevel("ERROR");s.conf().set(Specifications.ALLOW_SYNTHETIC,true);
  data=Pipeline.DATA;principals=Governance.PRINCIPALS;Pipeline.halt=false;PublicationTest.s=s;
  // The demo registry, plus a second operator and one principal holding both roles, to test separation of duties.
  Path registry=Files.createTempFile("principals",".json");
  Files.writeString(registry,"""
   [{"principal":"demo-operator","roles":["OPERATOR","INVESTIGATOR"]},{"principal":"ops-2","roles":["OPERATOR"]},
    {"principal":"demo-quality","roles":["QUALITY"]},{"principal":"both","roles":["OPERATOR","QUALITY"]},
    {"principal":"hospital-demo","roles":["RECIPIENT"],"recipients":["HOSPITAL-DEMO"]},{"principal":"warehouse-1","roles":["SITE"],"locations":["WAREHOUSE-1"]}]
   """);
  Governance.PRINCIPALS=registry.toString();
 }
 @AfterAll static void stop(){Pipeline.DATA=data;Governance.PRINCIPALS=principals;Pipeline.halt=true;s.stop();}
 static String lake()throws Exception {
  String d=PublicationTest.lake("governance");
  PublicationTest.run(d,PublicationTest.initial(),0,PublicationTest.LINEAGE,null);
  PublicationTest.run(d,PublicationTest.correction(),1,PublicationTest.LINEAGE,null);
  return d;
 }
 // The audit events of one action, as "principal outcome target".
 static List<String> audit(String action) {
  return Governance.events(s).filter(col("action").equalTo(action)).orderBy("at")
   .selectExpr("concat(principal,' ',outcome,' ',target)").as(Encoders.STRING()).collectAsList();
 }
 static void denied(Runnable r,String message) {
  ImpactQueries.AccessDenied e=assertThrows(ImpactQueries.AccessDenied.class,r::run);
  assertTrue(e.getMessage().contains(message),e.getMessage());
 }
 @Test void scopesComeFromTheRegistryAndEveryReadIsAudited()throws Exception {
  String d=lake();
  // A recipient cannot claim the investigator scope or another recipient, and an unknown caller gets nothing.
  denied(()->Governance.scope(s,"hospital-demo","investigator"),"scope investigator");
  denied(()->Governance.scope(s,"hospital-demo","recipient:HOSPITAL-2"),"scope recipient:HOSPITAL-2");
  denied(()->Governance.scope(s,"stranger","site:WAREHOUSE-1"),"stranger");
  ImpactQueries.Scope hospital=Governance.scope(s,"hospital-demo","recipient:HOSPITAL-DEMO");
  ImpactQueries.Page page=ImpactQueries.query(s,d,hospital,ImpactQueries.Filter.none(),50,null);
  assertFalse(page.rows().isEmpty());
  // Within its scope, a filter or a unit outside it is still refused, and audited.
  denied(()->ImpactQueries.query(s,d,hospital,new ImpactQueries.Filter(null,null,null,null,"HOSPITAL-2",null),50,null),"recipient HOSPITAL-2");
  Set<String> visible=new HashSet<>();page.rows().forEach(r->visible.add(r.getAs("productId")));
  String other=Snapshots.assessments(s,d,Snapshots.latest(s,d)).select("productId").as(Encoders.STRING()).collectAsList().stream().filter(u->!visible.contains(u)).findFirst().orElseThrow();
  denied(()->ImpactQueries.evidence(s,d,hospital,1,other),"evidence of "+other);
  ImpactQueries.evidence(s,d,Governance.scope(s,"demo-operator","investigator"),1,other);
  List<String> reads=audit("READ");
  assertEquals(List.of("hospital-demo DENIED investigator","hospital-demo DENIED recipient:HOSPITAL-2","stranger DENIED site:WAREHOUSE-1"),reads.subList(0,3));
  assertTrue(reads.get(3).startsWith("hospital-demo ALLOWED Filter["),reads.get(3));
  assertTrue(reads.get(4).startsWith("hospital-demo DENIED Filter["),reads.get(4));
  assertEquals(List.of("hospital-demo DENIED "+other,"demo-operator ALLOWED "+other),audit("READ_EVIDENCE"));
  Row allowed=Governance.events(s).filter("action = 'READ' AND outcome = 'ALLOWED'").first();
  assertTrue(allowed.<String>getAs("detail").contains("snapshot 1")&&allowed.<String>getAs("detail").contains("units "+page.rows().size()),allowed.<String>getAs("detail"));
  // Replay is an operator's action.
  denied(()->Governance.require(s,"demo-quality",Governance.Role.OPERATOR,"REPLAY",d),"requires OPERATOR");
  assertEquals(List.of("demo-quality DENIED "+d),audit("REPLAY"));
 }
 @Test void aRuleChangePublishesOnlyWithQualityApproval()throws Exception {
  String d=PublicationTest.lake("rules");
  PublicationTest.run(d,PublicationTest.initial(),0,PublicationTest.LINEAGE,null);
  s.conf().set(Specifications.ALLOW_SYNTHETIC,false);
  try {
   IllegalStateException e=assertThrows(IllegalStateException.class,()->PublicationTest.run(d,PublicationTest.correction(),1,PublicationTest.LINEAGE,null));
   assertTrue(e.getMessage().contains("CONFIGURATION_CHANGED")&&e.getMessage().contains("no quality approval"),e.getMessage());
   assertEquals(0L,(long)Snapshots.latest(s,d).getAs("snapshotId"),"nothing published");
   assertEquals(0L,Snapshots.uncommitted(s,d,Snapshots.HISTORY).count(),"nothing written");
   // An operator cannot approve it; quality can.
   denied(()->Governance.approve(s,"demo-operator","RULE_CHANGE",Governance.ruleSubject(false),null,"no"),"requires QUALITY");
   Governance.approve(s,"demo-quality","RULE_CHANGE",Governance.ruleSubject(false),null,"stop using synthetic thresholds");
   PublicationTest.run(d,PublicationTest.correction(),1,PublicationTest.LINEAGE,null);
   Row committed=Snapshots.latest(s,d);
   assertEquals(1L,(long)committed.getAs("snapshotId"));
   assertEquals("CONFIGURATION_CHANGED",committed.getAs("recomputeReason"));
  } finally {s.conf().set(Specifications.ALLOW_SYNTHETIC,true);}
  String subject=Governance.ruleSubject(false);
  assertEquals(List.of("pipeline DENIED "+subject,"pipeline APPLIED "+subject),audit("RULE_CHANGE"));
  assertEquals(List.of("demo-operator DENIED RULE_CHANGE "+subject,"demo-quality ALLOWED RULE_CHANGE "+subject),audit("APPROVE"));
  Row applied=Governance.events(s).filter("action = 'RULE_CHANGE' AND outcome = 'APPLIED'").first();
  assertTrue(applied.<String>getAs("detail").contains(Governance.approval(s,"RULE_CHANGE",subject).<String>getAs("approvalId")));
 }
 @Test void retentionPurgesOnlyApprovedSupersededHistory()throws Exception {
  String d=lake();
  Row newest=Snapshots.latest(s,d);
  long later=System.currentTimeMillis()+Duration.ofDays(400).toMillis();
  // Quality cannot plan; an operator plans snapshot 0 (everything older than 365 days, 400 days from now, but the newest).
  denied(()->Retention.plan(s,d,"demo-quality",Duration.ofDays(365),later),"requires OPERATOR");
  Retention.Plan plan=Retention.plan(s,d,"demo-operator",Duration.ofDays(365),later);
  assertEquals(List.of(0L),plan.snapshots());
  Dataset<Row> kept=Retention.kept(s,d);
  long superseded=Snapshots.table(s,d,Snapshots.HISTORY).filter("snapshotId = 0").join(kept,new String[]{"productId","snapshotId"},"left_anti").count();
  assertTrue(superseded>0,"the correction superseded some of snapshot 0's rows");
  assertEquals(superseded,plan.rows().get(Snapshots.HISTORY));
  // Not approved; approved by an operator; approved by its own planner: each refused and audited.
  denied(()->Retention.execute(s,d,"ops-2",plan.planId()),"not approved");
  denied(()->Governance.approve(s,"demo-operator","RETENTION",plan.planId(),"demo-operator","mine"),"requires QUALITY");
  Retention.Plan own=Retention.plan(s,d,"both",Duration.ofDays(365),later);
  denied(()->Governance.approve(s,"both","RETENTION",own.planId(),Retention.planner(s,own.planId()),"mine"),"requested by themselves");
  // Someone holding both roles may approve another operator's plan, but then may not execute it.
  Retention.Plan other=Retention.plan(s,d,"demo-operator",Duration.ofDays(365),later);
  Governance.approve(s,"both","RETENTION",other.planId(),Retention.planner(s,other.planId()),"older than the retention period");
  denied(()->Retention.execute(s,d,"both",other.planId()),"the approver may not execute");
  // Approved by quality; quality cannot execute; another operator can.
  Governance.approve(s,"demo-quality","RETENTION",plan.planId(),Retention.planner(s,plan.planId()),"older than the retention period");
  denied(()->Retention.execute(s,d,"demo-quality",plan.planId()),"requires OPERATOR");
  long deleted=Retention.execute(s,d,"ops-2",plan.planId());
  assertEquals(plan.rows().values().stream().mapToLong(Long::longValue).sum(),deleted);
  denied(()->Retention.execute(s,d,"ops-2",plan.planId()),"already executed");
  // Superseded rows of snapshot 0 are gone and the snapshot is refused; the newest state and its digests are unchanged.
  assertEquals(0L,Snapshots.table(s,d,Snapshots.HISTORY).filter("snapshotId = 0").join(kept,new String[]{"productId","snapshotId"},"left_anti").count());
  Row purged=Snapshots.table(s,d,Snapshots.COMMITS).filter("snapshotId = 0").first();
  assertNotNull(purged.getAs("purgedAt"));
  IllegalStateException e=assertThrows(IllegalStateException.class,()->Snapshots.assessments(s,d,purged).count());
  assertTrue(e.getMessage().contains("purged"),e.getMessage());
  Row after=Snapshots.latest(s,d);
  assertEquals(newest.<String>getAs("assessmentsDigest"),after.<String>getAs("assessmentsDigest"));
  Row recomputed=Snapshots.digests(Snapshots.state(s,d,after),Snapshots.intervals(s,d,after)).first();
  for(String digest:List.of("assessmentsDigest","evidenceDigest","intervalsDigest"))assertEquals(newest.<String>getAs(digest),recomputed.<String>getAs(digest),digest);
  // Nothing in Bronze or Silver was touched.
  String clean=PublicationTest.clean();Pipeline.DATA=d;   // clean() may run the pipeline elsewhere, which moves DATA and the audit log
  assertEquals(Snapshots.table(s,clean,"bronze/events").count(),Snapshots.table(s,d,"bronze/events").count());
  List<String> purges=audit("RETENTION_PURGE");
  assertEquals(List.of("ops-2 DENIED "+plan.planId(),"both DENIED "+other.planId(),"demo-quality DENIED "+plan.planId(),"ops-2 DONE "+plan.planId(),"ops-2 DENIED "+plan.planId()),purges);
  assertEquals(List.of("demo-quality DENIED "+d,"demo-operator ALLOWED "+plan.planId(),"both ALLOWED "+own.planId(),"demo-operator ALLOWED "+other.planId()),audit("RETENTION_PLAN"));
 }
}

package io.coldchain;
import java.io.*;
import java.nio.file.*;
import java.sql.Timestamp;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

// Who may do what, the audit log and approvals. Principals and their roles come from the deployment's registry
// (GOVERNANCE_PRINCIPALS, ops/governance/principals.json), reviewed like code. Operational work (replay, retention) and quality
// approval (rule, policy and configuration changes, retention plans) are separate roles, and an approval must come from
// someone other than the person whose action it approves. Every governed read, replay, rule change, approval and retention
// action, allowed or denied, is appended to audit/events in AUDIT_DIR, a volume of its own.
// Usage: Governance approve RULE_CHANGE|RETENTION <subject> <reason>; Governance audit [rows]. The caller is COLDCHAIN_PRINCIPAL.
public final class Governance {
 private Governance() {}
 enum Role{OPERATOR,QUALITY,INVESTIGATOR,RECIPIENT,SITE}
 record Principal(String name,Set<Role> roles,Set<String> recipients,Set<String> locations){}
 // Unset (tests), the audit log sits beside the lake.
 static String DIR=System.getenv("AUDIT_DIR");
 static String PRINCIPALS=System.getenv().getOrDefault("GOVERNANCE_PRINCIPALS","ops/governance/principals.json");
 static final String EVENTS="audit/events",APPROVALS="audit/approvals";
 static String dir(){return DIR!=null?DIR:Pipeline.DATA+"-audit";}
 static final StructType EVENT=new StructType().add("at","timestamp").add("principal","string").add("role","string").add("action","string")
  .add("target","string").add("outcome","string").add("detail","string");
 // Rule, policy and configuration changes need quality approval before a snapshot is computed under them.
 static final Set<String> RULE_CHANGES=Set.of("RULE_VERSION_CHANGED","POLICY_VERSION_CHANGED","CONFIGURATION_CHANGED");

 static String caller(){return System.getenv().getOrDefault("COLDCHAIN_PRINCIPAL",System.getenv().getOrDefault("USER","unknown"));}
 static Map<String,Principal> principals() {
  Map<String,Principal> out=new HashMap<>();
  try {
   for(JsonNode n:Simulator.JSON.readTree(new File(PRINCIPALS))) {
    Set<Role> roles=EnumSet.noneOf(Role.class);n.path("roles").forEach(r->roles.add(Role.valueOf(r.asText())));
    Set<String> recipients=new TreeSet<>(),locations=new TreeSet<>();
    n.path("recipients").forEach(r->recipients.add(r.asText()));n.path("locations").forEach(l->locations.add(l.asText()));
    out.put(n.path("principal").asText(),new Principal(n.path("principal").asText(),roles,recipients,locations));
   }
  } catch(IOException e){throw new UncheckedIOException("cannot read the principal registry "+PRINCIPALS,e);}
  return out;
 }
 static Principal principal(String name){return principals().get(name);}

 // Appends one audit event. Audit is part of the action: if it cannot be written, the action fails.
 static void audit(SparkSession s,String principal,Role role,String action,String target,String outcome,String detail) {
  s.createDataFrame(List.of(RowFactory.create(new Timestamp(System.currentTimeMillis()),principal,role==null?null:role.name(),action,target,outcome,detail)),EVENT)
   .write().format("delta").mode("append").save(dir()+"/"+EVENTS);
 }
 // The principal, if it holds the role; otherwise the denial is audited and thrown.
 static Principal require(SparkSession s,String name,Role role,String action,String target) {
  Principal p=principal(name);
  if(p==null||!p.roles().contains(role)) {
   audit(s,name,role,action,target,"DENIED",p==null?"unknown principal":"lacks role "+role);
   throw new ImpactQueries.AccessDenied(name+" may not "+action+" "+target+(p==null?": unknown principal":": requires "+role));
  }
  return p;
 }
 // The query scope a registered principal may use: the requested one when its roles grant it, otherwise denied and audited.
 static ImpactQueries.Scope scope(SparkSession s,String name,String requested) {
  Principal p=principal(name);
  ImpactQueries.Scope granted=p==null?null
   :requested.equals("investigator")&&p.roles().contains(Role.INVESTIGATOR)?ImpactQueries.Scope.investigator(name)
   :requested.startsWith("recipient:")&&p.roles().contains(Role.RECIPIENT)&&p.recipients().contains(requested.substring(10))?ImpactQueries.Scope.recipient(name,requested.substring(10))
   :requested.startsWith("site:")&&p.roles().contains(Role.SITE)&&p.locations().contains(requested.substring(5))?ImpactQueries.Scope.site(name,requested.substring(5))
   :null;
  if(granted==null) {
   audit(s,name,null,"READ",requested,"DENIED",p==null?"unknown principal":"scope not granted");
   throw new ImpactQueries.AccessDenied(name+" may not query with scope "+requested);
  }
  return granted;
 }

 // Records an approval. Only QUALITY approves, and never an action the approver requested (requestedBy).
 static void approve(SparkSession s,String approver,String action,String subject,String requestedBy,String reason) {
  require(s,approver,Role.QUALITY,"APPROVE",action+" "+subject);
  if(approver.equals(requestedBy)) {
   audit(s,approver,Role.QUALITY,"APPROVE",action+" "+subject,"DENIED","approver requested the action");
   throw new ImpactQueries.AccessDenied(approver+" may not approve "+action+" "+subject+" requested by themselves");
  }
  s.createDataFrame(List.of(RowFactory.create(UUID.randomUUID().toString(),action,subject,approver,requestedBy,new Timestamp(System.currentTimeMillis()),reason)),
   new StructType().add("approvalId","string").add("action","string").add("subject","string").add("approvedBy","string").add("requestedBy","string")
    .add("approvedAt","timestamp").add("reason","string"))
   .write().format("delta").mode("append").save(dir()+"/"+APPROVALS);
  audit(s,approver,Role.QUALITY,"APPROVE",action+" "+subject,"ALLOWED",reason);
 }
 // The newest approval of an action on a subject, or null.
 static Row approval(SparkSession s,String action,String subject) {
  if(!Snapshots.exists(s,dir(),APPROVALS))return null;
  List<Row> rows=Snapshots.table(s,dir(),APPROVALS).filter(col("action").equalTo(action).and(col("subject").equalTo(subject)))
   .orderBy(col("approvedAt").desc()).limit(1).collectAsList();
  return rows.isEmpty()?null:rows.get(0);
 }
 // The rules and configuration a snapshot is computed with, as quality approves them.
 static String ruleSubject(boolean allowSynthetic){return "rule="+Model.RULE_VERSION+";policy="+Policy.VERSION+";allowSyntheticSpecifications="+allowSynthetic;}
 // Before a snapshot computed under changed rules (reason): the approval, audited, or a refusal, audited and thrown.
 static void requireRuleApproval(SparkSession s,String reason,boolean allowSynthetic,long snapshot) {
  if(reason==null||!RULE_CHANGES.contains(reason))return;
  String subject=ruleSubject(allowSynthetic);
  Row a=approval(s,"RULE_CHANGE",subject);
  if(a==null) {
   audit(s,"pipeline",Role.OPERATOR,"RULE_CHANGE",subject,"DENIED",reason+" at snapshot "+snapshot+" without quality approval");
   throw new IllegalStateException("REFUSED snapshot "+snapshot+": "+reason+" to "+subject+" has no quality approval (Governance approve RULE_CHANGE)");
  }
  audit(s,"pipeline",Role.OPERATOR,"RULE_CHANGE",subject,"APPLIED",reason+" at snapshot "+snapshot+", approval "+a.getAs("approvalId")+" by "+a.getAs("approvedBy"));
 }
 static Dataset<Row> events(SparkSession s){return Snapshots.exists(s,dir(),EVENTS)?Snapshots.table(s,dir(),EVENTS):s.createDataFrame(List.of(),EVENT);}

 public static void main(String[] args) {
  SparkSession s=Pipeline.session("Cold-chain governance");s.sparkContext().setLogLevel("ERROR");
  String me=caller();
  try {
   if(args.length>=4&&args[0].equals("approve")) {
    // A retention plan names its planner; a rule change is requested by whoever deploys it, recorded in the reason.
    String requestedBy=args[1].equals("RETENTION")?Retention.planner(s,args[2]):null;
    approve(s,me,args[1],args[2],requestedBy,String.join(" ",Arrays.copyOfRange(args,3,args.length)));
    System.out.println("APPROVED "+args[1]+" "+args[2]+" by "+me);
   } else if(args.length>=1&&args[0].equals("audit")) {
    Principal p=principal(me);
    if(p==null||!(p.roles().contains(Role.OPERATOR)||p.roles().contains(Role.QUALITY)))require(s,me,Role.QUALITY,"READ_AUDIT","audit/events");
    audit(s,me,p.roles().contains(Role.QUALITY)?Role.QUALITY:Role.OPERATOR,"READ_AUDIT","audit/events","ALLOWED",null);
    events(s).orderBy(col("at").desc()).limit(args.length>1?Integer.parseInt(args[1]):50).toJSON().collectAsList().forEach(System.out::println);
   } else throw new IllegalArgumentException("usage: Governance approve RULE_CHANGE|RETENTION <subject> <reason> | Governance audit [rows]");
  } catch(ImpactQueries.AccessDenied e){System.out.println("DENIED: "+e.getMessage());s.stop();System.exit(3);}
  s.stop();
 }
}

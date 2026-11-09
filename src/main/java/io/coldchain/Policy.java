package io.coldchain;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

// The assessment policy: what each outcome means, which reasons lead to it, and the data-quality flags reported beside it.
// Gold's assessment SQL and gold/assessment_policies are both built from these lists, so they cannot disagree. No outcome is
// a release decision; product disposition stays with the quality unit.
public final class Policy {
 private Policy() {}
 public static final String VERSION="assessment-policy-v1";
 // An assessment covers one serialized unit over its whole retained history; shipment and episode scopes are not modeled.
 public static final String SCOPE="PRODUCT_RETAINED_HISTORY";
 // Inventory older than this, measured back from the newest source event in the snapshot, is stale.
 static final long MAX_INVENTORY_AGE_SECONDS=86_400;
 record Item(String code,String state,String when,String meaning) {}
 static final String REVIEW="QUALITY_REVIEW",INSUFFICIENT="INSUFFICIENT_DATA",NONE="NO_OBSERVED_EXCURSION";
 static final Map<String,String> STATES=new LinkedHashMap<>();
 static {
  STATES.put(REVIEW,"The evidence shows an excursion the policy does not allow. A quality reviewer must assess the product.");
  STATES.put(INSUFFICIENT,"The evidence cannot show that the product stayed within its specification.");
  STATES.put(NONE,"Complete, specified and calibrated measurement shows no excursion beyond the allowance, allowing for uncertainty.");
 }
 // Reasons in a fixed order. Any reason with state QUALITY_REVIEW makes the outcome QUALITY_REVIEW; any other reason makes it
 // INSUFFICIENT_DATA; no reason gives NO_OBSERVED_EXCURSION. `allowed` is the smallest allowance among matched specifications.
 static final List<Item> REASONS=List.of(
  new Item("EXCURSION_ABOVE_ALLOWANCE",REVIEW,"excursionSeconds > allowed","Cumulative excursion exceeds the smallest allowance of any specification that applied"),
  new Item("OUT_OF_RANGE_POINT",REVIEW,"outOfRangePoints > 0","A point reading is out of range: an observed excursion of unknown duration"),
  new Item("MISSING_COVERAGE",INSUFFICIENT,"missingSeconds > 0","Part of the time the unit was expected somewhere has no measurement"),
  new Item("MISSING_SPECIFICATION",INSUFFICIENT,"unspecifiedSeconds > 0","Part of the unit's located time has no applicable specification"),
  new Item("UNCALIBRATED_MEASUREMENT",INSUFFICIENT,"uncalibratedSeconds > 0","Some covered time is measured only by sensors without a valid calibration"),
  new Item("WITHIN_MEASUREMENT_UNCERTAINTY",INSUFFICIENT,"possibleExcursionSeconds > allowed AND NOT excursionSeconds > allowed",
   "Excursion is within the allowance, but not once measurement uncertainty is allowed for"));
 // Flags in a fixed order. They describe the evidence and never change the outcome; some repeat a reason, so a reviewer sees
 // the same conditions on every outcome.
 static final List<Item> FLAGS=List.of(
  new Item("MISSING_COVERAGE",null,"missingSeconds > 0","Some expected time has no measurement"),
  new Item("AMBIGUOUS_LINEAGE",null,"ambiguousLineage","A packaging chain from the unit uses a membership with a lineage anomaly"),
  new Item("AMBIGUOUS_ASSIGNMENT",null,"ambiguousAssignment","A container that held the unit had a sensor assigned to two containers at once"),
  new Item("AMBIGUOUS_CUSTODY",null,"ambiguousCustody","A container that held the unit had two custodians at once"),
  new Item("AMBIGUOUS_SPECIFICATION",null,"ambiguousSpecification","Specifications approved at the same instant overlap the unit's located time"),
  new Item("MIXED_SPECIFICATIONS",null,"size(specificationIds) > 1","More than one specification applied; see excursionBySpecification"),
  new Item("MISSING_CUSTODY",null,"missingCustodySeconds > 0","Some located time has no recorded custodian"),
  new Item("UNKNOWN_INVENTORY",null,"inventoryRecordedAt IS NULL","No inventory record exists for the unit"),
  new Item("STALE_INVENTORY",null,"inventoryRecordedAt IS NOT NULL AND (inventoryAgeSeconds > "+MAX_INVENTORY_AGE_SECONDS+" OR movedSinceInventory)",
   "The inventory record is older than the policy allows, or the unit moved after it was recorded"));
 static String codes(List<Item> items) {
  StringJoiner j=new StringJoiner(",","filter(array(","),r -> r IS NOT NULL)");
  for(Item i:items)j.add("IF(coalesce("+i.when()+",false),'"+i.code()+"',NULL)");
  return j.toString();
 }
 // The outcome and its reasons, flags and policy version, from a view of per-unit metrics.
 static String apply(String view) {
  return """
   SELECT *, CASE WHEN exists(assessmentReasons,r -> r IN (%1$s)) THEN '%2$s'
     WHEN size(assessmentReasons) > 0 OR allowed IS NULL THEN '%3$s' ELSE '%4$s' END assessment,
    '%5$s' policyVersion, '%6$s' assessmentScope
   FROM (SELECT *, %7$s assessmentReasons, %8$s dataQualityFlags FROM %9$s)
   """.formatted(String.join(",",REASONS.stream().filter(i->i.state().equals(REVIEW)).map(i->"'"+i.code()+"'").toList()),
    REVIEW,INSUFFICIENT,NONE,VERSION,SCOPE,codes(REASONS),codes(FLAGS),view);
 }
 static final StructType ENTRY=new StructType().add("code","string").add("state","string").add("condition","string").add("meaning","string");
 // One row describing this policy version, for gold/assessment_policies.
 public static Dataset<Row> table(SparkSession spark,String ruleVersion) {
  List<Row> states=new ArrayList<>();
  // impliesRelease is false for every state: the policy prioritizes review, it never releases or rejects product.
  STATES.forEach((k,v)->states.add(RowFactory.create(k,v,false)));
  StructType state=new StructType().add("state","string").add("meaning","string").add("impliesRelease","boolean");
  StructType schema=new StructType().add("policyVersion","string").add("ruleVersion","string").add("assessmentScope","string")
   .add("maxInventoryAgeSeconds","long").add("states",DataTypes.createArrayType(state))
   .add("reasons",DataTypes.createArrayType(ENTRY)).add("flags",DataTypes.createArrayType(ENTRY));
  return spark.createDataFrame(List.of(RowFactory.create(VERSION,ruleVersion,SCOPE,MAX_INVENTORY_AGE_SECONDS,states,rows(REASONS),rows(FLAGS))),schema);
 }
 static List<Row> rows(List<Item> items){return items.stream().map(i->RowFactory.create(i.code(),i.state(),i.when(),i.meaning())).toList();}
}

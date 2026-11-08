package io.coldchain;
import java.util.Arrays;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;

// Approved product specifications over time. Specifications for one product code may overlap; inside an overlap the one
// approved last (latest eventTime) applies. Candidates approved at the same instant cannot be ordered, so that window has
// no specification and is reported instead of resolved.
public final class Specifications {
 private Specifications() {}
 // Synthetic (demo) thresholds are used only where the session allows them (ALLOW_SYNTHETIC_SPECS); elsewhere they are ignored.
 public static final String ALLOW_SYNTHETIC="spark.coldchain.allowSyntheticSpecifications";
 static Dataset<Row> approved(Dataset<Row> events) {
  boolean allow=Boolean.parseBoolean(events.sparkSession().conf().get(ALLOW_SYNTHETIC,"false"));
  return events.filter("kind = 'SPEC'"+(allow?"":" AND NOT coalesce(synthetic,false)"));
 }
 // Elementary windows between consecutive boundaries of one product code, once per specification that covers the window.
 static Dataset<Row> windows(Dataset<Row> specs) {
  Dataset<Row> bounds=specs.selectExpr("productCode","explode(array(startTime,endTime)) startTime").distinct()
   .withColumn("endTime",lead("startTime",1).over(Window.partitionBy("productCode").orderBy("startTime"))).filter("endTime IS NOT NULL");
  WindowSpec w=Window.partitionBy("productCode","startTime");
  return bounds.as("w").join(specs.as("s"),expr("w.productCode = s.productCode AND s.startTime <= w.startTime AND w.endTime <= s.endTime"))
   .selectExpr("w.productCode","w.startTime","w.endTime","s.eventId","s.eventTime","s.minC","s.maxC","s.maxExcursionSeconds",
    "coalesce(s.synthetic,false) synthetic")
   .withColumn("latest",max("eventTime").over(w)).withColumn("candidates",sort_array(collect_list("eventId").over(w)))
   .withColumn("tied",sum(expr("IF(eventTime = latest,1,0)")).over(w));
 }
 // Joins rows that agree on every other column and touch in time into one interval.
 static Dataset<Row> touching(Dataset<Row> rows) {
  Column[] keys=Arrays.stream(rows.columns()).filter(c->!c.equals("startTime")&&!c.equals("endTime")).map(functions::col).toArray(Column[]::new);
  WindowSpec w=Window.partitionBy(keys).orderBy("startTime");
  Column[] groups=Arrays.copyOf(keys,keys.length+1);groups[keys.length]=col("island");
  return rows.withColumn("previousEnd",lag("endTime",1).over(w))
   .withColumn("island",sum(when(col("startTime").equalTo(col("previousEnd")),0).otherwise(1)).over(w))
   .groupBy(groups).agg(min("startTime").alias("startTime"),max("endTime").alias("endTime"))
   .drop("island");
 }
 // The specification that applies to each product code over time.
 public static Dataset<Row> effective(Dataset<Row> events) {
  return touching(windows(approved(events)).filter("eventTime = latest AND tied = 1")
   .selectExpr("productCode","eventId specificationId","minC","maxC","maxExcursionSeconds","synthetic","startTime","endTime"));
 }
 // Every window where specifications overlap, with the one that applies, or none when the latest approvals tie.
 public static Dataset<Row> overlaps(Dataset<Row> events) {
  return touching(windows(approved(events)).filter("size(candidates) > 1")
   .groupBy("productCode","startTime","endTime","candidates")
   .agg(max(expr("IF(eventTime = latest AND tied = 1,eventId,NULL)")).alias("specificationId"))
   .selectExpr("productCode","candidates specificationIds","specificationId",
    "IF(specificationId IS NULL,'UNRESOLVED_SAME_APPROVAL_TIME','LATEST_APPROVAL_APPLIES') resolution","startTime","endTime"));
 }
}

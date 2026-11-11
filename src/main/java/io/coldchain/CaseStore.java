package io.coldchain;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.Trigger;
import static org.apache.spark.sql.functions.*;

// The reviewers' case store: consumes review items from Kafka and keeps one case per unit. Delivery is at least once (the
// relay resends after a crash, and this consumer reprocesses a batch it did not finish), so every update is idempotent:
// a notification is recorded once per deliveryId, and a case takes an item only when the item's snapshot is newer than the
// one the case holds, so a duplicate or a late older revision changes nothing.
public final class CaseStore {
 private CaseStore() {}
 static String DIR=System.getenv().getOrDefault("CASES_DIR","/cases");
 static final String CASES="cases/review_items",NOTIFICATIONS="cases/review_notifications";
 public static void apply(Dataset<Row> items,String base,long batch) {
  SparkSession s=items.sparkSession();
  Dataset<Row> unique=items.dropDuplicates("deliveryId").localCheckpoint();
  if(unique.isEmpty())return;
  // The notification log: what reviewers were told, once per delivery, with when it first arrived.
  Pipeline.merge(s,unique.withColumn("receivedAt",current_timestamp()),base+"/"+NOTIFICATIONS,"t.deliveryId=s.deliveryId");
  Pipeline.inject("cases",batch);
  // The newest revision per case in this batch; an older one in the same batch is history the notification log keeps.
  Dataset<Row> newest=unique.withColumn("n",row_number().over(Window.partitionBy("reviewItemId").orderBy(col("snapshotId").desc()))).filter("n = 1").drop("n")
   .withColumn("status",expr("IF(change = 'WITHDRAWN','WITHDRAWN','OPEN')")).withColumn("updatedAt",current_timestamp());
  if(!Snapshots.exists(s,base,CASES))newest.write().format("delta").mode("overwrite").save(base+"/"+CASES);
  else DeltaTable.forPath(s,base+"/"+CASES).as("t").merge(newest.as("s"),"t.reviewItemId=s.reviewItemId")
   .whenMatched("s.snapshotId > t.snapshotId").updateAll().whenNotMatched().insertAll().execute();
 }
 static Dataset<Row> parse(Dataset<Row> kafka) {
  return kafka.select(from_json(col("value").cast("string"),Outbox.ITEM).alias("i")).select("i.*").filter("deliveryId IS NOT NULL");
 }
 public static void main(String[] args)throws Exception {
  SparkSession s=Pipeline.session("Cold-chain case store");
  s.readStream().format("kafka").option("kafka.bootstrap.servers",System.getenv().getOrDefault("KAFKA_BOOTSTRAP","kafka:9092")).options(KafkaClients.sparkOptions())
   .option("subscribe",Outbox.TOPIC).option("startingOffsets","earliest").option("failOnDataLoss",true).load()
   .writeStream().option("checkpointLocation",DIR+"/checkpoints/cases").trigger(Trigger.ProcessingTime("5 seconds"))
   .foreachBatch((VoidFunction2<Dataset<Row>,Long>)(batch,id)->apply(parse(batch),DIR,id)).start().awaitTermination();
 }
}

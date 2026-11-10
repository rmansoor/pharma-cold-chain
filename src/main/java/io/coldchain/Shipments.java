package io.coldchain;
import org.apache.spark.sql.*;

// Which units each shipment carried to which recipient. A line ships one unit, or a container: every unit resolved into
// that container (Lineage.unitContainers) at the dispatch instant. Recipients are organizations or sites, never patients.
public final class Shipments {
 private Shipments() {}
 public static Dataset<Row> lines(Dataset<Row> events,Dataset<Row> unitContainers) {
  SparkSession s=events.sparkSession();
  events.filter("kind = 'SHIPMENT'").createOrReplaceTempView("shipment_lines");
  unitContainers.selectExpr("productId unit","containerId box","startTime inFrom","endTime inTo","lineageIds")
   .createOrReplaceTempView("shipment_units");
  return s.sql("""
   SELECT l.shipmentId, l.eventId, l.revision, l.recipientId, l.productId, CAST(NULL AS string) containerId,
    CAST(NULL AS array<string>) lineageIds, l.startTime dispatchedAt, l.endTime deliveredAt
   FROM shipment_lines l WHERE l.productId IS NOT NULL
   UNION ALL
   SELECT l.shipmentId, l.eventId, l.revision, l.recipientId, u.unit, l.containerId, u.lineageIds, l.startTime, l.endTime
   FROM shipment_lines l JOIN shipment_units u ON u.box = l.containerId AND u.inFrom <= l.startTime AND l.startTime < u.inTo
   """);
 }
}

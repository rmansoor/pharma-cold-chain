package io.coldchain;

// Interval algebra for exposure. Intervals are half-open [startTime,endTime) at whole seconds. Every Gold duration is the
// length of a set of disjoint intervals built here, and each interval lists the exact source revisions behind it.
public final class Exposure {
 private Exposure() {}
 // Each key's intervals merged into islands: overlapping and touching intervals become one, so duplicate or overlapping
 // evidence (a retried reading, two sensors in one container) never counts the same second twice.
 static String islands(String view,String keys) {
  return """
   WITH previous AS (
    SELECT *, max(endTime) OVER (PARTITION BY %1$s ORDER BY startTime,endTime
     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) previousEnd FROM %2$s
   ), numbered AS (
    SELECT *, sum(IF(previousEnd IS NULL OR startTime > previousEnd,1,0)) OVER (PARTITION BY %1$s ORDER BY startTime,endTime
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) island FROM previous
   )
   SELECT %1$s, min(startTime) startTime, max(endTime) endTime FROM numbered GROUP BY %1$s, island
   """.formatted(keys,view);
 }
 // Total seconds in the union of each key's intervals.
 static String unionSeconds(String view,String keys) {
  return "SELECT "+keys+", sum(unix_timestamp(endTime)-unix_timestamp(startTime)) seconds FROM ("+islands(view,keys)+") GROUP BY "+keys;
 }
 // Time in one merged set and not in another, per product: (whole, part, result) names the sets. The boundaries of both
 // sets cut time into elementary windows; a window inside a whole island and inside no part island belongs to the result.
 static final String GAPS="('EXPECTED','COVERED','MISSING'),('COVERED','CALIBRATED','UNCALIBRATED'),"
  +"('LOCATED','SPECIFIED','UNSPECIFIED'),('LOCATED','CUSTODY','MISSING_CUSTODY')";
 static String gaps(String merged) {
  return """
   WITH pairs AS (SELECT * FROM VALUES %2$s AS pairs(whole,part,result)),
   sides AS (
    SELECT m.productId, p.result, m.metric = p.whole isWhole, m.startTime, m.endTime
    FROM %1$s m JOIN pairs p ON m.metric IN (p.whole,p.part)
   ), bounds AS (
    SELECT DISTINCT productId, result, t FROM sides LATERAL VIEW explode(array(startTime,endTime)) b AS t
   ), windows AS (
    SELECT productId, result, t startTime, lead(t) OVER (PARTITION BY productId,result ORDER BY t) endTime FROM bounds
   )
   SELECT w.productId, w.result metric, w.startTime, w.endTime FROM windows w
   LEFT SEMI JOIN sides o ON o.isWhole AND o.productId=w.productId AND o.result=w.result
    AND o.startTime <= w.startTime AND w.endTime <= o.endTime
   LEFT ANTI JOIN sides i ON NOT i.isWhole AND i.productId=w.productId AND i.result=w.result
    AND i.startTime <= w.startTime AND w.endTime <= i.endTime
   """.formatted(merged,GAPS);
 }
 // The set whose raw intervals explain each metric: a gap is explained by the whole it was cut from.
 static final String SOURCE="CASE metric WHEN 'MISSING' THEN 'EXPECTED' WHEN 'UNCALIBRATED' THEN 'COVERED' "
  +"WHEN 'UNSPECIFIED' THEN 'LOCATED' WHEN 'MISSING_CUSTODY' THEN 'LOCATED' ELSE metric END";
 // A checkpointed view keeps the column IDs of the view it came from (merged from intervals), so the side joined back is
 // re-aliased: Spark cannot always tell the two apart otherwise.
 // Each shaped interval with the source event IDs of every raw interval of its source set that overlaps it.
 static String explained(String shapes,String raw) {
  return """
   SELECT x.productId, x.metric, x.startTime, x.endTime, sort_array(array_distinct(flatten(collect_list(r.sourceIds)))) sourceIds
   FROM (SELECT *, %3$s source FROM %1$s) x
   JOIN (SELECT productId rProduct, metric rMetric, startTime rStart, endTime rEnd, sourceIds FROM %2$s) r
    ON r.rProduct=x.productId AND r.rMetric=x.source AND r.rStart < x.endTime AND x.startTime < r.rEnd
   GROUP BY x.productId, x.metric, x.startTime, x.endTime
   """.formatted(shapes,raw,SOURCE);
 }
 // Source event IDs replaced by the exact current revision of each event: kind, revision, semantic payload hash and the
 // Bronze coordinates it was read from. One row per key; rows without sources get an empty list.
 static String revisions(String view,String keys,String events) {
  keys=String.join(",",java.util.Arrays.stream(keys.split(",")).map(k->"v."+k.trim()).toList());
  return """
   SELECT %2$s, sort_array(collect_set(IF(e.id IS NULL,NULL,named_struct('kind',e.k,'eventId',e.id,'revision',e.rev,
    'payloadHash',e.hash,'topic',e.t,'partition',e.p,'offset',e.o)))) sources
   FROM (SELECT *, explode_outer(sourceIds) sourceId FROM %1$s) v
   LEFT JOIN (SELECT eventId id, kind k, revision rev, payloadHash hash, topic t, partition p, offset o FROM %3$s) e ON e.id=v.sourceId
   GROUP BY %2$s
   """.formatted(view,keys,events);
 }
}

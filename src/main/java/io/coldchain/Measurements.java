package io.coldchain;
import org.apache.spark.sql.*;

// Measurement quality: each reading's normalized value, calibration validity and uncertainty, and the time a sensor was
// assigned to a container without data.
public final class Measurements {
 private Measurements() {}
 // A point reading is calibrated when a calibration contains its instant; an interval reading by the seconds covered.
 public static Dataset<Row> readings(Dataset<Row> events) {
  SparkSession s=events.sparkSession();
  events.filter("kind = 'READING'").createOrReplaceTempView("quality_readings");
  events.filter("kind = 'CALIBRATION'").createOrReplaceTempView("quality_calibrations");
  s.sql("""
   SELECT r.eventId, c.eventId calibrationId, c.uncertaintyC,
    greatest(r.startTime,c.startTime) startTime, least(r.endTime,c.endTime) endTime
   FROM quality_readings r JOIN quality_calibrations c ON c.sensorId=r.sensorId
    AND (greatest(r.startTime,c.startTime) < least(r.endTime,c.endTime)
     OR (r.startTime = r.endTime AND c.startTime <= r.startTime AND r.startTime < c.endTime))
   """).createOrReplaceTempView("quality_matches");
  s.sql(Model.unionSeconds("quality_matches","eventId")).createOrReplaceTempView("quality_calibrated");
  return s.sql("""
   SELECT r.eventId, r.revision, r.sensorId, r.startTime, r.endTime, r.startTime = r.endTime point,
    r.temperatureC, r.temperature reportedTemperature, coalesce(r.temperatureUnit,'C') reportedUnit,
    m.calibrationIds, m.uncertaintyC, coalesce(c.seconds,0L) calibratedSeconds,
    CASE WHEN m.calibrationIds IS NULL THEN 'UNCALIBRATED'
     WHEN r.startTime = r.endTime OR c.seconds = unix_timestamp(r.endTime)-unix_timestamp(r.startTime) THEN 'CALIBRATED'
     ELSE 'PARTIALLY_CALIBRATED' END calibration
   FROM quality_readings r
   LEFT JOIN (SELECT eventId, sort_array(collect_set(calibrationId)) calibrationIds, max(uncertaintyC) uncertaintyC
    FROM quality_matches GROUP BY eventId) m USING(eventId)
   LEFT JOIN quality_calibrated c USING(eventId)
   """);
 }
 // Windows inside each current assignment with no interval reading from its sensor. Point readings do not fill a gap.
 public static Dataset<Row> samplingGaps(Dataset<Row> events) {
  SparkSession s=events.sparkSession();
  events.filter("kind = 'ASSIGNMENT'").createOrReplaceTempView("gap_assignments");
  events.filter("kind = 'READING' AND startTime < endTime").createOrReplaceTempView("gap_readings");
  return s.sql("""
   WITH clipped AS (
    SELECT a.eventId assignmentId, greatest(a.startTime,r.startTime) startTime, least(a.endTime,r.endTime) endTime
    FROM gap_assignments a JOIN gap_readings r ON r.sensorId=a.sensorId
    WHERE greatest(a.startTime,r.startTime) < least(a.endTime,r.endTime)
   ), ordered AS (
    SELECT *, max(endTime) OVER (PARTITION BY assignmentId ORDER BY startTime,endTime
     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) previousEnd FROM clipped
   ), bounds AS (
    SELECT assignmentId, min(startTime) firstStart, max(endTime) lastEnd FROM clipped GROUP BY assignmentId
   ), gaps AS (
    SELECT assignmentId, previousEnd startTime, startTime endTime FROM ordered WHERE startTime > previousEnd
    UNION ALL
    SELECT a.eventId, a.startTime, coalesce(b.firstStart,a.endTime) FROM gap_assignments a
    LEFT JOIN bounds b ON b.assignmentId=a.eventId WHERE coalesce(b.firstStart,a.endTime) > a.startTime
    UNION ALL
    SELECT a.eventId, b.lastEnd, a.endTime FROM gap_assignments a JOIN bounds b ON b.assignmentId=a.eventId WHERE b.lastEnd < a.endTime
   )
   SELECT g.assignmentId, a.sensorId, a.containerId, g.startTime, g.endTime,
    unix_timestamp(g.endTime)-unix_timestamp(g.startTime) seconds
   FROM gaps g JOIN gap_assignments a ON a.eventId=g.assignmentId
   """);
 }
}

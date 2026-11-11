package io.coldchain;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

// Silver: every Bronze record parsed, quarantine and conflict cases, canonical events and every table derived from them.
// A full build derives each table from all of Bronze. An incremental build starts from the Silver the previous snapshot
// published, parses only the Bronze records added since, and replaces in each table only the rows of the subjects those
// records can change; every other row is left as it is. The rows it writes equal those a full build writes (IncrementalTest).
public final class Silver {
 private Silver() {}
 static final String PARSED="silver/events_parsed",QUARANTINE="silver/quarantine",CASES="silver/conflict_cases",CASE_LOG="silver/case_log",
  CURRENT="silver/events_current",MEMBERSHIPS="silver/memberships",LINEAGE_ANOMALIES="silver/lineage_anomalies",UNITS="silver/unit_containers",
  TIMELINE_ANOMALIES="silver/timeline_anomalies",TIMELINES="silver/timelines",INVALIDATIONS="silver/mapping_invalidations",SHIPMENTS="silver/shipments",
  SPECIFICATIONS="silver/specifications",OVERLAPS="silver/specification_overlaps",READINGS="silver/readings",GAPS="silver/sampling_gaps";
 // The tables a build writes, in order. The case log is an append-only audit outside this list: it only ever gains rows.
 static final List<String> TABLES=List.of(PARSED,QUARANTINE,CASES,CURRENT,MEMBERSHIPS,LINEAGE_ANOMALIES,UNITS,TIMELINE_ANOMALIES,TIMELINES,
  INVALIDATIONS,SHIPMENTS,SPECIFICATIONS,OVERLAPS,READINGS,GAPS);
 // How Silver was built; changes are the canonical changes since the previous snapshot when the build found them (incremental).
 record Result(String mode,String reason,Dataset<Row> changes,long canonicalMs) {}
 // Why Silver must be built in full, or null: everything Gold needs in full (Snapshots.fullReason), and a previous snapshot that
 // recorded the Bronze version and every Silver table it was built from.
 static String fullReason(Row previous,String code,boolean allowSynthetic) {
  String reason=Snapshots.fullReason(previous,code,allowSynthetic);
  if(reason!=null)return reason;
  if(Snapshots.recorded(previous,"bronze/events")==null||TABLES.stream().anyMatch(t->Snapshots.recorded(previous,t)==null))return "PREVIOUS_SILVER_FORMAT";
  return null;
 }
 // Builds Silver from source (Bronze at the version recorded in versions) and records every version written.
 static Result build(SparkSession s,String base,long id,Row previous,Dataset<Row> source,Map<String,Long> versions,String reason) {
  return reason==null?incremental(s,base,id,previous,source,versions):full(s,base,id,source,versions,reason);
 }
 static Dataset<Row> at(SparkSession s,String base,String table,long version){return s.read().format("delta").option("versionAsOf",version).load(base+"/"+table);}
 static Dataset<Row> head(SparkSession s,String base,String table){return Snapshots.table(s,base,table);}
 static Result full(SparkSession s,String base,long id,Dataset<Row> source,Map<String,Long> versions,String reason) {
  long started=System.nanoTime();
  BiConsumer<Dataset<Row>,String> write=(data,table)->versions.put(table,Pipeline.snapshot(data,base+"/"+table));
  Dataset<Row> parsed=Model.parse(source).cache();
  try {
   write.accept(parsed,PARSED);
   Dataset<Row> good=parsed.filter("valid");
   Dataset<Row> quarantine=Model.quarantine(parsed),cases=Model.conflictCases(good);
   write.accept(quarantine,QUARANTINE);
   write.accept(cases,CASES);
   Pipeline.recordCases(Model.caseStates(quarantine,cases),id,base+"/"+CASE_LOG);
   versions.put(CASE_LOG,Pipeline.version(s,base+"/"+CASE_LOG));
   write.accept(Model.canonical(good),CURRENT);
   Pipeline.inject("canonical",id);
   long canonicalMs=(System.nanoTime()-started)/1_000_000;
   // Pin canonical input by reading the materialized version for every derived table.
   Dataset<Row> current=at(s,base,CURRENT,versions.get(CURRENT));
   Dataset<Row> edges=Lineage.edges(current).cache(),anomalies=Lineage.anomalies(edges).cache(),units=Lineage.unitContainers(edges,anomalies).cache();
   Dataset<Row> timeline=Timelines.anomalies(current).cache(),history=Timelines.history(good,timeline).cache();
   try {
    write.accept(Lineage.memberships(edges,anomalies),MEMBERSHIPS);
    write.accept(anomalies,LINEAGE_ANOMALIES);
    write.accept(units,UNITS);
    write.accept(timeline,TIMELINE_ANOMALIES);
    write.accept(history,TIMELINES);
    write.accept(Timelines.invalidations(history,units),INVALIDATIONS);
    write.accept(Shipments.lines(current,units),SHIPMENTS);
    write.accept(Specifications.effective(current),SPECIFICATIONS);
    write.accept(Specifications.overlaps(current),OVERLAPS);
    write.accept(Measurements.readings(current),READINGS);
    write.accept(Measurements.samplingGaps(current),GAPS);
   } finally {edges.unpersist();anomalies.unpersist();units.unpersist();timeline.unpersist();history.unpersist();}
   return new Result("FULL",reason,null,canonicalMs);
  } finally {parsed.unpersist();}
 }
 // Replaces, in one commit, the rows of a table whose key columns match keys with rows (the recomputed rows of those keys),
 // and records the table's version. A key with no new row is deleted; rows of other keys are untouched.
 static void replace(SparkSession s,String base,String table,Dataset<Row> rows,Dataset<Row> keys,Map<String,Long> versions,String... cols) {
  Dataset<Row> k=keys.select(Arrays.stream(cols).map(c->col(c).as(c)).toArray(Column[]::new)).filter(Arrays.stream(cols).map(c->c+" IS NOT NULL").collect(Collectors.joining(" AND ")))
   .distinct();
  // Delete markers match the rows they replace; recomputed rows never match and are inserted.
  Dataset<Row> source=rows.withColumn("deleteKey",lit(false)).unionByName(k.withColumn("deleteKey",lit(true)),true).localCheckpoint();
  if(!source.isEmpty()) {
   Map<String,String> insert=new LinkedHashMap<>();
   for(String c:rows.columns())insert.put(c,"s."+c);
   DeltaTable.forPath(s,base+"/"+table).as("t").merge(source.as("s"),"s.deleteKey AND "+Arrays.stream(cols).map(c->"t."+c+" = s."+c).collect(Collectors.joining(" AND ")))
    .whenMatched().delete().whenNotMatched("NOT s.deleteKey").insertExpr(insert).execute();
  }
  versions.put(table,Pipeline.version(s,base+"/"+table));
 }
 static Dataset<Row> ids(Dataset<Row> rows,String column,String as){return rows.select(col(column).as(as)).filter(col(as).isNotNull()).distinct();}
 // Every ID reachable from start along the packaging graph, from the `from` end of each membership to its `to` end. As far
 // as Lineage.MAX_HOPS memberships, the length Lineage.paths searches.
 static Dataset<Row> reach(Dataset<Row> graph,Dataset<Row> start,String from,String to) {
  Dataset<Row> set=start.localCheckpoint();
  for(int hop=0;hop<Lineage.MAX_HOPS;hop++) {
   Dataset<Row> next=set.union(graph.join(set.withColumnRenamed("id",from),from).select(col(to).alias("id"))).distinct().localCheckpoint();
   if(next.count()==set.count())break;
   set=next;
  }
  return set;
 }
 // The incremental build. Each table's rows belong to a subject (an event, a packaging ID, a sensor, a container or a product
 // code); a table's rows are replaced for every subject whose rows a changed canonical event can alter, recomputed from all
 // the events those rows depend on.
 static Result incremental(SparkSession s,String base,long id,Row previous,Dataset<Row> source,Map<String,Long> versions) {
  long started=System.nanoTime();
  // Start from the Silver the previous snapshot published: whatever an attempt that stopped before its manifest wrote is undone.
  for(String t:TABLES) {
   long v=Snapshots.recorded(previous,t);
   if(Pipeline.version(s,base+"/"+t)!=v)DeltaTable.forPath(s,base+"/"+t).restoreToVersion(v);
  }
  // Records added since the previous snapshot. Silver was restored to that snapshot, so none of them is in it yet.
  String[] coordinates={"topic","partition","offset"};
  Dataset<Row> published=at(s,base,"bronze/events",Snapshots.recorded(previous,"bronze/events")).select("topic","partition","offset");
  Dataset<Row> added=Model.parse(source.join(published,coordinates,"left_anti")).localCheckpoint();
  added.write().format("delta").mode("append").save(base+"/"+PARSED);
  versions.put(PARSED,Pipeline.version(s,base+"/"+PARSED));
  // Quarantine, conflict cases and canonical selection depend only on the records of one eventId: they are recomputed from
  // every record of each eventId that gained one. A record without an eventId is quarantined on its own and never changes.
  Dataset<Row> keys=ids(added,"eventId","eventId").localCheckpoint();
  Dataset<Row> parsed=head(s,base,PARSED).join(keys,"eventId","left_semi").localCheckpoint(),good=parsed.filter("valid");
  Dataset<Row> quarantine=Model.quarantine(parsed.unionByName(added.filter("eventId IS NULL"))).localCheckpoint(),cases=Model.conflictCases(good).localCheckpoint();
  replace(s,base,QUARANTINE,quarantine,keys,versions,"eventId");
  replace(s,base,CASES,cases,keys,versions,"eventId");
  Pipeline.recordCases(Model.caseStates(quarantine,cases),id,base+"/"+CASE_LOG);
  versions.put(CASE_LOG,Pipeline.version(s,base+"/"+CASE_LOG));
  replace(s,base,CURRENT,Model.canonical(good),keys,versions,"eventId");
  Pipeline.inject("canonical",id);
  long canonicalMs=(System.nanoTime()-started)/1_000_000;
  Dataset<Row> before=at(s,base,CURRENT,Snapshots.recorded(previous,CURRENT)),current=at(s,base,CURRENT,versions.get(CURRENT));
  // Only eventIds that gained a record can have a different canonical row.
  Dataset<Row> changes=Impact.changes(before.join(keys,"eventId","left_semi"),current.join(keys,"eventId","left_semi")).localCheckpoint();
  // Both sides of every change: a corrected event affects the subjects it named before as well as the ones it names now.
  Dataset<Row> sides=changes.selectExpr("old v").union(changes.selectExpr("new v")).filter("v IS NOT NULL").selectExpr("v.*").localCheckpoint();
  Dataset<Row> containers=lineage(s,base,current,sides,versions);
  timelines(s,base,current,sides,keys,containers,versions);
  // Shipment lines: changed shipments, and shipments of containers whose resolved units changed.
  Dataset<Row> shipments=ids(changes.filter("kind = 'SHIPMENT'"),"eventId","eventId")
   .union(ids(current.filter("kind = 'SHIPMENT'").join(containers,"containerId","left_semi"),"eventId","eventId")).distinct().localCheckpoint();
  replace(s,base,SHIPMENTS,Shipments.lines(current.join(shipments,"eventId","left_semi"),head(s,base,UNITS)),shipments,versions,"eventId");
  // Specifications and their overlaps: per product code.
  Dataset<Row> codes=ids(sides.filter("kind = 'SPEC'"),"productCode","productCode").localCheckpoint();
  Dataset<Row> specs=current.filter("kind = 'SPEC'").join(codes,"productCode","left_semi");
  replace(s,base,SPECIFICATIONS,Specifications.effective(specs),codes,versions,"productCode");
  replace(s,base,OVERLAPS,Specifications.overlaps(specs),codes,versions,"productCode");
  // Readings: changed readings, and every reading of a sensor whose calibrations changed, with all calibrations of their sensors.
  Dataset<Row> readings=ids(changes.filter("kind = 'READING'"),"eventId","eventId")
   .union(ids(current.filter("kind = 'READING'").join(ids(sides.filter("kind = 'CALIBRATION'"),"sensorId","sensorId"),"sensorId","left_semi"),"eventId","eventId"))
   .distinct().localCheckpoint();
  Dataset<Row> chosen=current.filter("kind = 'READING'").join(readings,"eventId","left_semi").localCheckpoint();
  replace(s,base,READINGS,Measurements.readings(chosen.unionByName(current.filter("kind = 'CALIBRATION'").join(ids(chosen,"sensorId","sensorId"),"sensorId","left_semi"))),
   readings,versions,"eventId");
  // Sampling gaps: changed assignments, and every assignment of a sensor whose readings changed, with all readings of their sensors.
  Dataset<Row> assignments=ids(changes.filter("kind = 'ASSIGNMENT'"),"eventId","assignmentId")
   .union(ids(current.filter("kind = 'ASSIGNMENT'").join(ids(sides.filter("kind = 'READING'"),"sensorId","sensorId"),"sensorId","left_semi"),"eventId","assignmentId"))
   .distinct().localCheckpoint();
  Dataset<Row> assigned=current.filter("kind = 'ASSIGNMENT'").join(assignments.withColumnRenamed("assignmentId","eventId"),"eventId","left_semi").localCheckpoint();
  replace(s,base,GAPS,Measurements.samplingGaps(assigned.unionByName(current.filter("kind = 'READING'").join(ids(assigned,"sensorId","sensorId"),"sensorId","left_semi"))),
   assignments,versions,"assignmentId");
  return new Result("INCREMENTAL",null,changes,canonicalMs);
 }
 // Packaging lineage. The scope is every ID below an ID that a changed membership names (on either side), and every ID above
 // those, in the current graph. A changed anomaly is about an ID a changed membership names, or a cycle through one, and a changed container path
 // starts at a unit below one, so every row that can change belongs to a scope ID; every membership naming a scope ID is
 // read, so the rows of scope IDs are exact. Rows about other IDs computed on the way are dropped. Returns the containers
 // whose resolved units changed.
 static Dataset<Row> lineage(SparkSession s,String base,Dataset<Row> current,Dataset<Row> sides,Map<String,Long> versions) {
  Dataset<Row> seeds=ids(sides.filter("kind = 'PLACEMENT'"),"productId","id").union(ids(sides.filter("kind = 'PLACEMENT'"),"containerId","id"))
   .union(ids(sides.filter("kind = 'PACKING'"),"childId","id")).union(ids(sides.filter("kind = 'PACKING'"),"parentId","id")).distinct().localCheckpoint();
  if(seeds.isEmpty()) {
   for(String t:List.of(MEMBERSHIPS,LINEAGE_ANOMALIES,UNITS))versions.put(t,Pipeline.version(s,base+"/"+t));
   return seeds.withColumnRenamed("id","containerId");
  }
  Dataset<Row> edges=Lineage.edges(current);
  // The current graph is enough: both IDs of a changed membership's former value are seeds already.
  Dataset<Row> graph=edges.select("childId","parentId").distinct().localCheckpoint();
  Dataset<Row> scope=reach(graph,reach(graph,seeds,"parentId","childId"),"childId","parentId").localCheckpoint();
  Dataset<Row> local=edges.join(scope.withColumnRenamed("id","childId"),"childId","left_semi")
   .unionByName(edges.join(scope.withColumnRenamed("id","parentId"),"parentId","left_semi")).distinct().localCheckpoint();
  Dataset<Row> anomalies=Lineage.anomalies(local).join(scope.withColumnRenamed("id","subjectId"),"subjectId","left_semi").localCheckpoint();
  Dataset<Row> children=scope.withColumnRenamed("id","childId");
  replace(s,base,MEMBERSHIPS,Lineage.memberships(local.join(children,"childId","left_semi"),anomalies),
   ids(head(s,base,MEMBERSHIPS).join(children,"childId","left_semi"),"eventId","eventId"),versions,"eventId");
  replace(s,base,LINEAGE_ANOMALIES,anomalies,scope.withColumnRenamed("id","subjectId"),versions,"subjectId");
  Dataset<Row> units=scope.withColumnRenamed("id","productId");
  Dataset<Row> old=head(s,base,UNITS).join(units,"productId","left_semi").localCheckpoint();
  Dataset<Row> now=Lineage.unitContainers(local,anomalies).join(units,"productId","left_semi").localCheckpoint();
  replace(s,base,UNITS,now,units,versions,"productId");
  Dataset<Row> aligned=now.select(Arrays.stream(old.columns()).map(functions::col).toArray(Column[]::new));
  return ids(old.exceptAll(aligned).unionByName(aligned.exceptAll(old)),"containerId","containerId").localCheckpoint();
 }
 // Sensor and custody contradictions are per sensor and per container. An event's timeline rows depend on its revisions and
 // the contradictions it takes part in; a mapping invalidation on its assignment's revisions and the units of its containers.
 static void timelines(SparkSession s,String base,Dataset<Row> current,Dataset<Row> sides,Dataset<Row> keys,Dataset<Row> containers,Map<String,Long> versions) {
  Dataset<Row> sensors=ids(sides.filter("kind = 'ASSIGNMENT'"),"sensorId","sensorId").localCheckpoint();
  Dataset<Row> custody=ids(sides.filter("kind = 'CUSTODY'"),"containerId","containerId").localCheckpoint();
  Dataset<Row> subjects=sensors.selectExpr("'SENSOR_IN_TWO_CONTAINERS' anomaly","sensorId subjectId")
   .union(custody.selectExpr("'CUSTODY_OVERLAP' anomaly","containerId subjectId")).localCheckpoint();
  Dataset<Row> old=head(s,base,TIMELINE_ANOMALIES).join(subjects,new String[]{"anomaly","subjectId"},"left_semi").localCheckpoint();
  Dataset<Row> now=Timelines.anomalies(current.filter("kind = 'ASSIGNMENT'").join(sensors,"sensorId","left_semi")
   .unionByName(current.filter("kind = 'CUSTODY'").join(custody,"containerId","left_semi"))).localCheckpoint();
  replace(s,base,TIMELINE_ANOMALIES,now,subjects,versions,"anomaly","subjectId");
  // Events whose revisions changed, and events whose contradictions did.
  Dataset<Row> events=keys.union(old.selectExpr("explode(eventIds) eventId")).union(now.selectExpr("explode(eventIds) eventId")).distinct().localCheckpoint();
  replace(s,base,TIMELINES,Timelines.history(head(s,base,PARSED).filter("valid").join(events,"eventId","left_semi"),head(s,base,TIMELINE_ANOMALIES)),
   events,versions,"eventId");
  // Assignments whose revisions changed, and assignments that ever named a container whose resolved units changed.
  Dataset<Row> history=head(s,base,TIMELINES).filter("kind = 'ASSIGNMENT'");
  Dataset<Row> assignments=keys.select(col("eventId").as("assignmentId")).union(ids(history.join(containers,"containerId","left_semi"),"eventId","assignmentId"))
   .distinct().localCheckpoint();
  replace(s,base,INVALIDATIONS,Timelines.invalidations(history.join(assignments.withColumnRenamed("assignmentId","eventId"),"eventId","left_semi"),head(s,base,UNITS)),
   assignments,versions,"assignmentId");
 }
 // The Silver table versions a snapshot recorded.
 static Map<String,Long> versions(Row commit) {
  Map<String,Long> v=new LinkedHashMap<>();
  for(String t:TABLES)if(Snapshots.recorded(commit,t)!=null)v.put(t,Snapshots.recorded(commit,t));
  return v;
 }
 // Silver tables that differ between two builds (each read at the given versions), compared row for row regardless of order:
 // how Replay and IncrementalTest check an incremental build against a full one. A table the first did not record is skipped.
 static List<String> differences(SparkSession s,String base,Map<String,Long> versions,String otherBase,Map<String,Long> other) {
  List<String> differ=new ArrayList<>();
  Dataset<Row> a=null,b=null;
  for(String t:TABLES) {
   Long v=versions.get(t),w=other.get(t);
   if(v==null)continue;
   if(w==null){differ.add(t);continue;}
   Dataset<Row> x=rows(at(s,base,t,v)).withColumn("table",lit(t)),y=rows(at(s,otherBase,t,w)).withColumn("table",lit(t));
   a=a==null?x:a.union(x);b=b==null?y:b.union(y);
  }
  // One comparison of all tables at once: each table's rows tagged with its name.
  if(a!=null)differ.addAll(a.exceptAll(b).union(b.exceptAll(a)).select("table").distinct().as(Encoders.STRING()).collectAsList());
  return differ.stream().sorted(Comparator.comparingInt(TABLES::indexOf)).toList();
 }
 static Dataset<Row> rows(Dataset<Row> table) {
  return table.select(to_json(struct(Arrays.stream(table.columns()).sorted().map(functions::col).toArray(Column[]::new))).alias("row"));
 }
}

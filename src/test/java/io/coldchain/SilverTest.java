package io.coldchain;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.coldchain.ModelTest.with;
class SilverTest {
 static SparkSession s;
 @BeforeAll static void start(){s=Pipeline.session("cold-chain-silver-tests");s.sparkContext().setLogLevel("ERROR");}
 @AfterAll static void stop(){s.stop();}
 static String json(Object event)throws Exception{return event instanceof String t?t:Simulator.JSON.writeValueAsString(event);}
 // The simulator's initial state plus extra records, parsed as they would be read back from Bronze.
 static Dataset<Row> parsed(Object... extra)throws Exception {
  List<String> raw=new ArrayList<>();for(var e:Simulator.initial())raw.add(json(e));for(var e:extra)raw.add(json(e));
  return ModelTest.parseRaw(raw,expr(ModelTest.TOPIC));
 }
 static Row only(Dataset<Row> rows,String condition){List<Row> r=rows.filter(condition).collectAsList();assertEquals(1,r.size(),condition);return r.get(0);}
 @Test void quarantineCarriesEvidenceAndResolvesOnCorrection()throws Exception {
  String hot=json(with(6,"eventId","late","temperatureC",150.0));
  Dataset<Row> q=Model.quarantine(parsed(hot,"{broken"));
  assertEquals(2,q.count());
  Row bad=only(q,"eventId = 'late'");
  assertEquals(List.of("OUT_OF_RANGE:temperatureC"),bad.getList(bad.fieldIndex("reasons")));
  assertEquals(hot,bad.getAs("raw"));assertEquals("coldchain.sensors",bad.getAs("topic"));assertEquals("test",bad.getAs("sourceSystem"));
  assertNotNull(bad.getAs("offset"));assertNotNull(bad.getAs("caseId"));assertEquals("OPEN",bad.getAs("status"));
  Row broken=only(q,"raw = '{broken'");assertNull(broken.getAs("eventId"));assertEquals("OPEN",broken.getAs("status"));
  // The source corrects the reading with a higher revision.
  Dataset<Row> corrected=parsed(hot,"{broken",with(6,"eventId","late","revision",2));
  Dataset<Row> q2=Model.quarantine(corrected);
  Row resolved=only(q2,"eventId = 'late'");
  assertEquals("RESOLVED",resolved.getAs("status"));assertEquals("SUPERSEDED_BY_VALID_REVISION",resolved.getAs("resolution"));
  assertEquals(2L,(long)resolved.getAs("resolvedByRevision"));assertEquals(bad.<String>getAs("caseId"),resolved.getAs("caseId"));
  assertEquals("OPEN",only(q2,"raw = '{broken'").getAs("status"),"unattributable input has no automatic resolution");
  assertEquals(2L,(long)only(Model.canonical(corrected.filter("valid")),"eventId = 'late'").getAs("revision"));
  // A valid retry at the same revision does not resolve the invalid record.
  assertEquals("OPEN",only(Model.quarantine(parsed(hot,with(6,"eventId","late"))),"eventId = 'late'").getAs("status"));
 }
 @Test void conflictCaseListsEveryVariantAndOnlyAHigherRevisionResolvesIt()throws Exception {
  Map<String,Object> rival=with(3,"containerId","BOX-B");
  Dataset<Row> open=Model.conflictCases(parsed(rival).filter("valid"));
  Row c=only(open,"eventId = 'assign'");
  assertEquals("PAYLOAD_MISMATCH",c.getAs("caseType"));assertEquals(1L,(long)c.getAs("revision"));assertEquals("OPEN",c.getAs("status"));
  List<Row> variants=c.getList(c.fieldIndex("variants"));
  assertEquals(2,variants.size());
  assertEquals(2,variants.stream().map(v->v.getAs("payloadHash")).distinct().count());
  assertEquals(2,variants.stream().map(v->v.getAs("offset")).distinct().count());
  assertEquals(0,Model.canonical(parsed(rival).filter("valid")).filter("eventId = 'assign'").count(),"no arbitrary winner");
  // The source issues revision 2; the disputed revision 1 is superseded and revision 2 is promoted.
  Dataset<Row> fixed=parsed(rival,with(3,"revision",2,"containerId","BOX-B")).filter("valid");
  Row r=only(Model.conflictCases(fixed),"eventId = 'assign'");
  assertEquals("RESOLVED",r.getAs("status"));assertEquals("SUPERSEDED_BY_HIGHER_REVISION",r.getAs("resolution"));
  assertEquals(2L,(long)r.getAs("resolvedByRevision"));assertEquals(c.<String>getAs("caseId"),r.getAs("caseId"));
  Row promoted=only(Model.canonical(fixed),"eventId = 'assign'");
  assertEquals(2L,(long)promoted.getAs("revision"));assertEquals("BOX-B",promoted.getAs("containerId"));
  // An ID reused for another kind stays withheld, even after a higher revision of the original kind.
  Dataset<Row> reused=parsed(with(4,"revision",2,"kind","PLACEMENT","productCode","DRUG-DEMO","lotId","LOT-1","containerId","BOX-A",
   "startTime","2026-01-01T12:00:00Z","endTime","2026-01-01T12:10:00Z"),with(4,"revision",3)).filter("valid");
  Row k=only(Model.conflictCases(reused),"eventId = 'inventory-A'");
  assertEquals("KIND_MISMATCH",k.getAs("caseType"));assertEquals("OPEN",k.getAs("status"));assertNull(k.getAs("revision"));
  assertEquals(3,k.getList(k.fieldIndex("variants")).size());
  assertEquals(0,Model.canonical(reused).filter("eventId = 'inventory-A'").count());
 }
 @Test void caseLogRecordsEachTransitionOnceWithTheSnapshotThatSawIt(@TempDir Path dir)throws Exception {
  Map<String,Object> rival=with(3,"containerId","BOX-B");String hot=json(with(6,"eventId","late","temperatureC",150.0));
  Dataset<Row> before=parsed(rival,hot),after=parsed(rival,hot,with(3,"revision",2,"containerId","BOX-B"));
  Dataset<Row> states0=Model.caseStates(Model.quarantine(before),Model.conflictCases(before.filter("valid")));
  Dataset<Row> states1=Model.caseStates(Model.quarantine(after),Model.conflictCases(after.filter("valid")));
  Pipeline.recordCases(states0,0,dir.toString());Pipeline.recordCases(states0,0,dir.toString());
  Pipeline.recordCases(states1,1,dir.toString());Pipeline.recordCases(states1,1,dir.toString());
  Dataset<Row> log=s.read().format("delta").load(dir.toString());
  assertEquals(3,log.count(),"replayed snapshots add nothing");
  assertEquals(List.of("OPEN@0","RESOLVED@1"),log.filter("eventId = 'assign'").orderBy("snapshotId")
   .selectExpr("concat(status,'@',snapshotId)").as(Encoders.STRING()).collectAsList());
  Row invalid=only(log,"caseType = 'INVALID_CONTRACT'");assertEquals("OPEN",invalid.getAs("status"));assertEquals(0L,(long)invalid.getAs("snapshotId"));
 }
}

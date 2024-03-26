package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BufferedRecordsOracleTest {
    private final OracleHelper oracleHelper = new OracleHelper();

    private Map<Object, Object> props;

    @Before
    public void setUp() throws IOException, SQLException {
        oracleHelper.setUp();
        props = new HashMap<>();
        props.put("name", "T_Limit2Ods_V3");
        props.put("connector.class", "vn.bnh.connect.jdbc.JdbcAuditSinkConnector");
        props.put("tasks.max", "1");
        props.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        props.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        props.put("transforms", "filterField");
        props.put("errors.retry.timeout", "1000");
        props.put("errors.retry.delay.max.ms", "1000");
        props.put("errors.tolerance", "none");
        props.put("errors.log.enable", "true");
        props.put("errors.log.include.messages", "true");
        props.put("topics", "ODS_LIMIT_HIST");
        props.put("errors.deadletterqueue.topic.name", "ODS_LIMIT_DLQ");
        props.put("errors.deadletterqueue.topic.replication.factor", "2");
        props.put("errors.deadletterqueue.context.headers.enable", "true");
        props.put("transforms.filterField.type", "org.apache.kafka.connect.transforms.ReplaceField$Value");
        props.put("transforms.filterField.exclude", "CURRENT_TS, REP_TS, OP_TS, LOOKUP_KEY, TABLE_NAME, ROWKEY, RECVER");
        props.put("connection.url", "jdbc:oracle:thin:@dc2uatdbdev.seauat.com.vn:1521/ODS_DEV");
        props.put("connection.user", "T24BNK");
        props.put("connection.password", "n4ngC#pth3K");
        props.put("dialect.name", "OracleDatabaseDialect");
        props.put("connection.attempts", "1");
        props.put("connection.backoff.ms", "5000");
        props.put("insert.mode", "UPSERT");
        props.put("table.types", "TABLE");
        props.put("table.name.format", "T24BNK.ODS_LIMIT");
        props.put("pk.mode", "record_value");
        props.put("pk.fields", "RECID");
        props.put("auto.create", "false");
        props.put("auto.evolve", "false");
        props.put("delete.mode", "UPDATE");
        props.put("delete.as.update.identifier", "COMMIT_ACTION=D");
        props.put("delete.as.update.value.schema", "COMMIT_TS, REPLICAT_TS, STREAM_TS");
        props.put("delete.as.update.key", "RECID");
        props.put("audit.timestamp.column", "TIME_UPDATE");
        props.put("hist.table.record.status.key", "RECID");
        props.put("hist.table.record.status.identifier", "RECORD_STATUS=REVE");
        props.put("hist.table.record.status.value.schema", "COMMIT_TS, REPLICAT_TS, COMMIT_ACTION_HIS, STREAM_TS, AUTHORISER_HIS, INPUTTER_HIS, DATE_TIME_AUTH_HIS, DATE_TIME_INPUT_HIS, RECORD_STATUS");
        props.put("principal.service.name", "kafkaldap");
        props.put("principal.service.password", "Vhht@dmin#2023");
        props.put("value.converter.schema.registry.url", "http://dc1-dev-kkschm01:8081");
        props.put("key.converter.schema.registry.url", "http://dc1-dev-kkschm01:8081");
    }


    @After
    public void tearDown() throws IOException, SQLException {
        oracleHelper.tearDown();
    }

    @Test
    public void insertData() throws SQLException {
        final JdbcSinkConfig config = new JdbcSinkConfig(props);

        final String url = oracleHelper.oracleUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);

        final TableId tableId = new TableId(null, null, "ODS_LIMIT");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, oracleHelper.connection);

        // RECID -> STRING_SCHEMA (not null)
        // string -> OPTIONAL_STRING_SCHEMA
        // int -> OPTIONAL_INT32_SCHEMA
        // long -> OPTIONAL_INT64_SCHEMA
        // double -> OPTIONAL_FLOAT64_SCHEMA

        final Schema schema = SchemaBuilder.struct()
                .field("RECID", Schema.STRING_SCHEMA)
                .field("COMMIT_TS", Schema.OPTIONAL_INT64_SCHEMA)
                .field("REPLICAT_TS", Schema.OPTIONAL_INT64_SCHEMA)
                .field("STREAM_TS", Schema.OPTIONAL_INT64_SCHEMA)
                .field("COMMIT_SCN", Schema.OPTIONAL_INT64_SCHEMA)
                .field("COMMIT_ACTION", Schema.OPTIONAL_STRING_SCHEMA)
                .field("REQUEST_TYPE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("DEBIT_ACCOUNT", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CUSTOMER_NO", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CHARGE_DATE", Schema.OPTIONAL_INT32_SCHEMA)
                .field("CHARGE_CCY", Schema.OPTIONAL_STRING_SCHEMA)
                .field("TOTAL_CHG_AMT", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("RELATED_REF", Schema.OPTIONAL_STRING_SCHEMA)
                .field("MSG_SERIES", Schema.OPTIONAL_STRING_SCHEMA)
                .field("SWIFT_TO_RAISE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CHARGE_DETAIL", Schema.OPTIONAL_STRING_SCHEMA)
                .field("EXTRA_DETAILS", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ORD_INST_TYPE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ORDERING_INST", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ACCT_WITH_TYPE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ACCT_WITH_BANK", Schema.OPTIONAL_STRING_SCHEMA)
                .field("SENDER_DETAIL", Schema.OPTIONAL_STRING_SCHEMA)
                .field("SENDER_INFO", Schema.OPTIONAL_STRING_SCHEMA)
                .field("STATUS", Schema.OPTIONAL_STRING_SCHEMA)
                .field("REMARKS", Schema.OPTIONAL_STRING_SCHEMA)
                .field("REMOVE_DATE", Schema.OPTIONAL_INT32_SCHEMA)
                .field("SEAB_BROKER_TYPE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("SEAB_SALES_TYPE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("SEAB_BROKER_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("SEAB_SALE_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("INPUTTER", Schema.OPTIONAL_STRING_SCHEMA)
                .field("DATE_TIME_INPUT", Schema.OPTIONAL_INT64_SCHEMA)
                .field("DATE_TIME_AUTHORISE", Schema.OPTIONAL_INT64_SCHEMA)
                .field("AUTHORISER", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CO_CODE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("DEPT_CODE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CHARGE_CODE1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CHARGE_CODE2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CHARGE_CODE3", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CHARGE_CODE4", Schema.OPTIONAL_STRING_SCHEMA)
                .field("BANKING_DATE", Schema.OPTIONAL_INT32_SCHEMA).build();

        final Struct value = new Struct(schema)
                .put("RECID", "CHG2324417004540")
                .put("COMMIT_TS", 1710499756000L)
                .put("REPLICAT_TS", 1710499759466L)
                .put("STREAM_TS", 1710499761746L)
                .put("COMMIT_SCN", 9963997365508L)
                .put("COMMIT_ACTION", "I")
                .put("REQUEST_TYPE", "BOOK")
                .put("DEBIT_ACCOUNT", "000007120138")
                .put("CUSTOMER_NO", "16494366")
                .put("CHARGE_DATE", 19601)
                .put("CHARGE_CCY", "VND")
                .put("TOTAL_CHG_AMT", 6160000.0)
                .put("RELATED_REF", "0964782275")
                .put("MSG_SERIES", "1")
                .put("SWIFT_TO_RAISE", "")
                .put("CHARGE_DETAIL", "ACC")
                .put("EXTRA_DETAILS", "Thu phi mo TKTT so dep")
                .put("ORD_INST_TYPE", "")
                .put("ORDERING_INST", "")
                .put("ACCT_WITH_TYPE", "")
                .put("ACCT_WITH_BANK", "")
                .put("SENDER_DETAIL", "")
                .put("SENDER_INFO", "")
                .put("STATUS", "PAID")
                .put("REMARKS", "")
                .put("REMOVE_DATE", 19601)
                .put("SEAB_BROKER_TYPE", "")
                .put("SEAB_SALES_TYPE", "")
                .put("SEAB_BROKER_ID", "")
                .put("SEAB_SALE_ID", "")
                .put("INPUTTER", "44211_USM5013000001_I_INAU_OFS_O.AP")
                .put("DATE_TIME_INPUT", 1710499740000L)
                .put("DATE_TIME_AUTHORISE", 1710499740000L)
                .put("AUTHORISER", "44211_USM5013000001_OFS_O.API.INT")
                .put("CO_CODE", "VN0010902")
                .put("DEPT_CODE", "1")
                .put("CHARGE_CODE1", "1SD9V6H2O")
                .put("CHARGE_CODE2", null)
                .put("CHARGE_CODE3", null)
                .put("CHARGE_CODE4", null)
                .put("BANKING_DATE", 19591);

        final SinkRecord record = new SinkRecord("ODS_LIMIT", 0, null, null, schema, value, 0);

        assertEquals(Collections.emptyList(), buffer.add(record));
        assertEquals(Collections.singletonList(record), buffer.flush());
    }
}

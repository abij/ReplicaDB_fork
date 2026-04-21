package org.replicadb.db2;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.PathItem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.ReplicaDB;
import org.replicadb.cli.ToolOptions;
import org.replicadb.config.ReplicadbAzuriteContainer;
import org.replicadb.config.ReplicadbDB2Container;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class DB22ADLSGen2FileTest {
    private static final Logger LOG = LogManager.getLogger(DB22ADLSGen2FileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    /** abfss:// URI with Azurite well-known credentials embedded in the host part. */
    private static final String SINK_CONNECT =
            "abfss://" + ReplicadbAzuriteContainer.TEST_FILESYSTEM
            + "@" + ReplicadbAzuriteContainer.ACCOUNT_NAME + ".dfs.core.windows.net";

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    private static ReplicadbAzuriteContainer azurite;

    private Connection db2Conn;
    private DataLakeFileSystemClient fsClient;

    @BeforeAll
    static void setUpContainers() {
        azurite = ReplicadbAzuriteContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        this.fsClient = azurite.getTestFilesystemClient();
        FileManager.setTempFilesPath(new HashMap<>());
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Delete all files written during this test
        for (PathItem item : fsClient.listPaths()) {
            fsClient.deleteFileIfExists(item.getName());
        }
        this.db2Conn.close();
        FileManager.setTempFilesPath(new HashMap<>());
    }

    // -------------------------------------------------------------------------
    // Sanity checks
    // -------------------------------------------------------------------------

    @Test
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        assertTrue(rs.getString(1).contains("1"));
    }

    @Test
    void testAzuriteConnection() {
        assertTrue(fsClient.exists(), "Azurite test filesystem should exist");
    }

    @Test
    void testDb2SourceRows() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t_source");
        rs.next();
        assertEquals(EXPECTED_ROWS, rs.getInt(1));
    }

    // -------------------------------------------------------------------------
    // CSV tests
    // -------------------------------------------------------------------------

    @Test
    void testDb22ADLSGen2CsvComplete() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT + "/db22adls2_csv_test.csv";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sinkUrl,
                "--sink-file-format", FileFormats.CSV.getType()
        };
        ToolOptions options = new ToolOptions(args);
        options.setSinkConnectionParams(buildSinkParams());

        assertEquals(0, ReplicaDB.processReplica(options));

        List<PathItem> paths = listAllPaths();
        LOG.info("ADLS Gen2 objects after CSV replication: {}", paths.stream().map(PathItem::getName).collect(Collectors.toList()));
        assertTrue(paths.stream().anyMatch(p -> p.getName().contains("db22adls2_csv_test")),
                "Expected CSV file in Azurite");
    }

    @Test
    void testDb22ADLSGen2CsvCompleteParallel() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT + "/db22adls2_csv_parallel.csv";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sinkUrl,
                "--sink-file-format", FileFormats.CSV.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        options.setSinkConnectionParams(buildSinkParams());

        assertEquals(0, ReplicaDB.processReplica(options));

        // 4 task files + 1 stats file
        List<PathItem> paths = listAllPaths();
        long csvCount = paths.stream().filter(p -> p.getName().contains("db22adls2_csv_parallel")).count();
        assertEquals(4, csvCount, "Expected 4 task CSV files for --jobs=4");
    }

    // -------------------------------------------------------------------------
    // Parquet tests
    // -------------------------------------------------------------------------

    @Test
    void testDb22ADLSGen2ParquetComplete() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT + "/db22adls2_test.parquet";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sinkUrl,
                "--sink-file-format", FileFormats.PARQUET.getType()
        };
        ToolOptions options = new ToolOptions(args);
        options.setSinkConnectionParams(buildSinkParams());

        assertEquals(0, ReplicaDB.processReplica(options));

        List<PathItem> paths = listAllPaths();
        LOG.info("ADLS Gen2 objects after Parquet replication: {}", paths.stream().map(PathItem::getName).collect(Collectors.toList()));
        assertTrue(paths.stream().anyMatch(p -> p.getName().endsWith(".parquet")),
                "Expected a .parquet file in Azurite");
    }

    @Test
    void testDb22ADLSGen2ParquetCompleteParallel() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT + "/db22adls2_parallel.parquet";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sinkUrl,
                "--sink-file-format", FileFormats.PARQUET.getType(),
                "--jobs", "4"
        };
        ToolOptions options = new ToolOptions(args);
        options.setSinkConnectionParams(buildSinkParams());

        assertEquals(0, ReplicaDB.processReplica(options));

        List<PathItem> paths = listAllPaths();
        long parquetCount = paths.stream().filter(p -> p.getName().endsWith(".parquet")).count();
        assertEquals(4, parquetCount, "Expected 4 task Parquet files for --jobs=4");
    }

    // -------------------------------------------------------------------------
    // Stats JSON test
    // -------------------------------------------------------------------------

    @Test
    void testDb22ADLSGen2StatsJson() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT + "/stats_test.parquet";

        String[] args = {
                "--options-file", RESOURCE_DIR + REPLICADB_CONF_FILE,
                "--source-connect", db2.getJdbcUrl(),
                "--source-user", db2.getUsername(),
                "--source-password", db2.getPassword(),
                "--sink-connect", sinkUrl,
                "--sink-file-format", FileFormats.PARQUET.getType()
        };
        ToolOptions options = new ToolOptions(args);
        options.setSinkConnectionParams(buildSinkParams());

        assertEquals(0, ReplicaDB.processReplica(options));

        List<PathItem> paths = listAllPaths();
        boolean statsExists = paths.stream().anyMatch(p -> p.getName().contains("_replicadb_stats.json"));
        assertTrue(statsExists, "Expected _replicadb_stats.json in Azurite");

        // Download and parse stats file to verify totalRows
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        fsClient.getFileClient("_replicadb_stats.json").read(buf);
        byte[] statsBytes = buf.toByteArray();
        JsonNode stats = new ObjectMapper().readTree(statsBytes);
        LOG.info("Stats JSON: {}", stats.toPrettyString());
        assertEquals(EXPECTED_ROWS, stats.get("totalRows").asInt(),
                "Stats totalRows should match source row count");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Properties buildSinkParams() {
        Properties p = new Properties();
        p.setProperty("accountKey", ReplicadbAzuriteContainer.ACCOUNT_KEY);
        p.setProperty("endpoint", azurite.getDfsEndpoint());
        return p;
    }

    private List<PathItem> listAllPaths() {
        return fsClient.listPaths()
                .stream()
                .collect(Collectors.toList());
    }
}

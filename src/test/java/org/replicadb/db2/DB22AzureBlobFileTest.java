package org.replicadb.db2;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
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

import java.io.ByteArrayOutputStream;
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
class DB22AzureBlobFileTest {
    private static final Logger LOG = LogManager.getLogger(DB22AzureBlobFileTest.class);
    private static final String RESOURCE_DIR = Paths.get("src", "test", "resources").toFile().getAbsolutePath();
    private static final String REPLICADB_CONF_FILE = "/replicadb.conf";
    private static final int EXPECTED_ROWS = 4097;

    /** Blob URL pointing at the Azurite devstoreaccount1 — endpoint is overridden at test time. */
    private static final String SINK_CONNECT_BASE =
            "https://" + ReplicadbAzuriteContainer.ACCOUNT_NAME
            + ".blob.core.windows.net/" + ReplicadbAzuriteContainer.TEST_FILESYSTEM;

    @Rule
    public static Db2Container db2 = ReplicadbDB2Container.getInstance();

    private static ReplicadbAzuriteContainer azurite;

    private Connection db2Conn;
    private BlobContainerClient containerClient;

    @BeforeAll
    static void setUpContainers() {
        azurite = ReplicadbAzuriteContainer.getInstance();
    }

    @BeforeEach
    void before() throws SQLException {
        this.db2Conn = DriverManager.getConnection(db2.getJdbcUrl(), db2.getUsername(), db2.getPassword());
        this.containerClient = azurite.getBlobContainerClient();
        FileManager.setTempFilesPath(new HashMap<>());
    }

    @AfterEach
    void tearDown() throws SQLException {
        for (BlobItem item : containerClient.listBlobs()) {
            containerClient.getBlobClient(item.getName()).deleteIfExists();
        }
        this.db2Conn.close();
        FileManager.setTempFilesPath(new HashMap<>());
    }

    @Test
    void testDb2Connection() throws SQLException {
        Statement stmt = db2Conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");
        rs.next();
        assertTrue(rs.getString(1).contains("1"));
    }

    @Test
    void testAzuriteConnection() {
        assertTrue(containerClient.exists(), "Azurite test container should exist");
    }

    @Test
    void testDb22AzureBlobCsvComplete() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT_BASE + "/db22blob_csv_test.csv";

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

        List<BlobItem> blobs = listAllBlobs();
        LOG.info("Azure Blob objects after CSV replication: {}", blobs.stream().map(BlobItem::getName).collect(Collectors.toList()));
        assertTrue(blobs.stream().anyMatch(b -> b.getName().contains("db22blob_csv_test")),
                "Expected CSV blob in Azurite");
    }

    @Test
    void testDb22AzureBlobCsvCompleteParallel() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT_BASE + "/db22blob_csv_parallel.csv";

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

        long csvCount = listAllBlobs().stream()
                .filter(b -> b.getName().contains("db22blob_csv_parallel")).count();
        assertEquals(4, csvCount, "Expected 4 task CSV blobs for --jobs=4");
    }

    @Test
    void testDb22AzureBlobParquetComplete() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT_BASE + "/db22blob_test.parquet";

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

        List<BlobItem> blobs = listAllBlobs();
        LOG.info("Azure Blob objects after Parquet replication: {}", blobs.stream().map(BlobItem::getName).collect(Collectors.toList()));
        assertTrue(blobs.stream().anyMatch(b -> b.getName().endsWith(".parquet")),
                "Expected a .parquet blob in Azurite");
    }

    @Test
    void testDb22AzureBlobParquetCompleteParallel() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT_BASE + "/db22blob_parallel.parquet";

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

        long parquetCount = listAllBlobs().stream()
                .filter(b -> b.getName().endsWith(".parquet")).count();
        assertEquals(4, parquetCount, "Expected 4 task Parquet blobs for --jobs=4");
    }

    @Test
    void testDb22AzureBlobStatsJson() throws ParseException, IOException {
        String sinkUrl = SINK_CONNECT_BASE + "/stats_test.parquet";

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

        List<BlobItem> blobs = listAllBlobs();
        assertTrue(blobs.stream().anyMatch(b -> b.getName().contains("_replicadb_stats.json")),
                "Expected _replicadb_stats.json in Azurite");

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        containerClient.getBlobClient("_replicadb_stats.json").downloadStream(buf);
        JsonNode stats = new ObjectMapper().readTree(buf.toByteArray());
        LOG.info("Stats JSON: {}", stats.toPrettyString());
        assertEquals(EXPECTED_ROWS, stats.get("totalRows").asInt(),
                "Stats totalRows should match source row count");
    }

    private Properties buildSinkParams() {
        Properties p = new Properties();
        p.setProperty("accountKey", ReplicadbAzuriteContainer.ACCOUNT_KEY);
        p.setProperty("endpoint", azurite.getBlobEndpoint());
        return p;
    }

    private List<BlobItem> listAllBlobs() {
        return containerClient.listBlobs().stream().collect(Collectors.toList());
    }
}

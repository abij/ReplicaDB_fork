package org.replicadb.manager;

import com.azure.core.credential.TokenCredential;
import com.azure.core.util.BinaryData;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.file.FileFormats;
import org.replicadb.manager.file.FileManager;
import org.replicadb.manager.file.FileManagerFactory;

import java.io.OutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Sink manager for Azure Data Lake Storage Gen2.
 *
 * <p>Uses {@code azure-storage-file-datalake} (DFS endpoint) with {@code azure-identity}
 * for credential resolution.
 *
 * <p><b>Connection string format:</b>
 * <pre>abfss://filesystem@accountname.dfs.core.windows.net/path/to/output.parquet</pre>
 *
 * <h3>Authentication — resolved in this order</h3>
 * <ol>
 *   <li><b>Account key</b> — set {@code accountKey} in sink connection params.
 *       Simple, but not recommended for production.</li>
 *   <li><b>Service principal</b> — set {@code tenantId}, {@code clientId},
 *       {@code clientSecret} in sink connection params.</li>
 *   <li><b>Default Azure credential chain</b> — no params needed. Tries in order:
 *       environment variables, workload identity, managed identity, Azure CLI,
 *       Azure Developer CLI, Azure PowerShell. Recommended for AKS / cloud deployments.</li>
 * </ol>
 *
 * <h3>Write strategy by format</h3>
 * <ul>
 *   <li><b>CSV</b> — streamed directly via {@code DataLakeFileOutputStream}. No local buffer.</li>
 *   <li><b>Parquet / ORC</b> — written to a local temp file first (immutable format with
 *       deferred footer), then uploaded via {@code uploadFromFile()} using parallel block upload.</li>
 * </ul>
 *
 * <h3>Statistics</h3>
 * After all tasks complete, {@code postSinkTasks()} writes {@code _replicadb_stats.json}
 * to the same ADLS Gen2 directory with per-task row counts, file names, durations, and totals.
 *
 * <p>When {@code --jobs > 1} each task produces a separate file with a {@code _taskId} suffix.
 */
public class ADLSGen2Manager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(ADLSGen2Manager.class.getName());
    private static final String STATS_FILE_NAME = "_replicadb_stats.json";
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    // Shared across all instances in a single JVM run (same pattern as FileManager.tempFilesPath).
    // Per-task managers populate this; the main manager reads it in postSinkTasks().
    private static final ConcurrentHashMap<Integer, TaskStats> taskStats = new ConcurrentHashMap<>();

    // Parsed from URI
    private String accountName;
    private String fileSystemName;
    private String serviceEndpoint;
    private String filePath;
    private String statsFilePathOverride; // null = derive from filePath

    // Authentication — at most one will be non-null
    private String accountKey;       // option 1: storage account key
    private String tenantId;         // option 2: service principal
    private String clientId;
    private String clientSecret;
    // option 3: DefaultAzureCredential (no fields needed)

    private final FileManager fileManager;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public ADLSGen2Manager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
        loadAdlsProperties();
        this.fileManager = new FileManagerFactory().accept(opts, dsType);
    }

    private void loadAdlsProperties() {
        Properties props = dsType == DataSourceType.SOURCE
                ? options.getSourceConnectionParams()
                : options.getSinkConnectionParams();

        // Auth params — all optional; credential is resolved in priority order at runtime
        this.accountKey   = props.getProperty("accountKey");
        this.tenantId     = props.getProperty("tenantId");
        this.clientId     = props.getProperty("clientId");
        this.clientSecret = props.getProperty("clientSecret");

        // Parse:  abfss://filesystem@accountname.dfs.core.windows.net/path/to/file
        try {
            URI uri = new URI(options.getSinkConnect());
            this.fileSystemName = uri.getUserInfo();          // part before @
            String host = uri.getHost();                      // accountname.dfs.core.windows.net
            this.accountName = host.substring(0, host.indexOf('.'));
            this.serviceEndpoint = "https://" + host;

            // Allow endpoint override — used for Azurite (local tests) and sovereign clouds.
            // Example: endpoint=http://127.0.0.1:10000/devstoreaccount1
            String endpointOverride = props.getProperty("endpoint");
            if (endpointOverride != null && !endpointOverride.isEmpty()) {
                this.serviceEndpoint = endpointOverride;
            }

            String uriPath = uri.getPath();
            String derivedPath = uriPath.startsWith("/") ? uriPath.substring(1) : uriPath;

            String keyFileNameProp = props.getProperty("keyFileName");
            this.filePath = (keyFileNameProp != null && !keyFileNameProp.isEmpty())
                    ? keyFileNameProp
                    : derivedPath;

            // Optional: override where the stats JSON is written.
            // Value is a path within the same filesystem, e.g. meta/result.json
            String statsFile = props.getProperty("statsFile");
            this.statsFilePathOverride = (statsFile != null && !statsFile.isEmpty()) ? statsFile : null;

        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid ADLS Gen2 connection string. Expected: abfss://filesystem@account.dfs.core.windows.net/path", e);
        }

        LOG.debug("ADLS Gen2 — endpoint: {}, filesystem: {}, path: {}, auth: {}",
                serviceEndpoint, fileSystemName, filePath, describeAuth());
    }

    // -------------------------------------------------------------------------
    // Pre / post lifecycle
    // -------------------------------------------------------------------------

    /**
     * Validates the sink path and checks that no target files already exist,
     * then clears the shared stats map so a fresh run starts clean.
     *
     * <p>Fails fast with a clear error before any data is read or written if:
     * <ul>
     *   <li>The sink URI has no file path (root of container is not a valid sink).</li>
     *   <li>Any of the target output files already exist in ADLS Gen2.</li>
     * </ul>
     */
    @Override
    public Future<Integer> preSinkTasks(ExecutorService executor) throws Exception {
        validateSinkPath();
        taskStats.clear();
        return null;
    }

    private void validateSinkPath() {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException(
                    "ADLS Gen2 sink path is missing. Specify a file path in the URI, " +
                    "e.g. abfss://filesystem@account.dfs.core.windows.net/path/to/output.parquet");
        }

        DataLakeFileSystemClient fsClient = buildServiceClient().getFileSystemClient(fileSystemName);
        int jobs = options.getJobs();

        for (int taskId = 0; taskId < jobs; taskId++) {
            String targetPath = resolveTaskPath(filePath, taskId);
            if (fsClient.getFileClient(targetPath).exists()) {
                throw new IllegalStateException(
                        "ADLS Gen2 sink file already exists: " + targetPath +
                        ". Delete it first or choose a different destination path.");
            }
        }
    }

    /**
     * Writes {@code _replicadb_stats.json} to the same ADLS Gen2 directory as the data files.
     */
    @Override
    public void postSinkTasks() throws Exception {
        if (taskStats.isEmpty()) {
            LOG.debug("No task stats recorded — skipping stats file");
            return;
        }

        ObjectNode root = MAPPER.createObjectNode();
        root.put("timestamp", Instant.now().toString());
        root.put("source", options.getSourceConnect());
        root.put("sink", options.getSinkConnect());
        root.put("mode", options.getMode());
        root.put("jobs", options.getJobs());

        ArrayNode tasksNode = root.putArray("tasks");
        long totalRows = 0;

        for (Map.Entry<Integer, TaskStats> entry : taskStats.entrySet()
                .stream()
                .sorted(Comparator.comparingInt(Map.Entry::getKey))
                .toList()) {
            TaskStats s = entry.getValue();
            ObjectNode t = tasksNode.addObject();
            t.put("taskId", entry.getKey());
            t.put("file", s.filePath);
            t.put("rows", s.rows);
            t.put("durationMs", s.durationMs);
            totalRows += s.rows;
        }

        root.put("totalRows", totalRows);

        byte[] json = MAPPER.writeValueAsBytes(root);
        String statsPath = statsFilePathOverride != null ? statsFilePathOverride : statsFilePath(filePath);
        LOG.info("Writing replication stats to ADLS Gen2: {}", statsPath);

        buildFileClient(statsPath).upload(BinaryData.fromBytes(json), true);
    }

    // -------------------------------------------------------------------------
    // Core write
    // -------------------------------------------------------------------------

    @Override
    public String getDriverClass() {
        return JdbcDrivers.ADLS2.getDriverClass();
    }

    @Override
    protected Connection makeSinkConnection() {
        return null;
    }

    @Override
    protected void truncateTable() {
    }

    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) throws Exception {
        String targetPath = resolveTaskPath(filePath, taskId);
        LOG.info("Writing to ADLS Gen2: filesystem={}, path={}", fileSystemName, targetPath);

        DataLakeFileClient fileClient = buildFileClient(targetPath);
        long start = System.currentTimeMillis();
        int processedRows;

        if (isStreamable()) {
            // CSV: stream row-by-row directly to ADLS Gen2.
            // DataLakeFileOutputStream uses the DFS append+flush protocol internally.
            fileClient.create(true); // create/overwrite the file before streaming
            try (OutputStream os = fileClient.getOutputStream()) {
                processedRows = fileManager.writeData(os, resultSet, taskId, null);
            }
        } else {
            // Parquet / ORC: immutable format with deferred footer — must write to a local
            // temp file first, then upload the complete file via parallel block upload.
            processedRows = fileManager.writeData(OutputStream.nullOutputStream(), resultSet, taskId, null);
            String tempFilePath = FileManager.getTempFilePath(taskId);
            LOG.debug("Uploading temp file to ADLS Gen2: {}", tempFilePath);
            fileClient.uploadFromFile(tempFilePath, true);
        }

        long durationMs = System.currentTimeMillis() - start;
        taskStats.put(taskId, new TaskStats(targetPath, processedRows, durationMs));

        LOG.info("Uploaded {} rows → abfss://{}.dfs.core.windows.net/{}/{} ({}ms)",
                processedRows, accountName, fileSystemName, targetPath, durationMs);
        return processedRows;
    }

    // -------------------------------------------------------------------------
    // Credential resolution
    // -------------------------------------------------------------------------

    /**
     * Builds a {@link DataLakeServiceClient} using the appropriate credential.
     *
     * <p>Priority:
     * <ol>
     *   <li>Storage account key ({@code accountKey} param) — uses {@link StorageSharedKeyCredential}</li>
     *   <li>Service principal ({@code tenantId} + {@code clientId} + {@code clientSecret}) —
     *       uses {@link com.azure.identity.ClientSecretCredential}</li>
     *   <li>Default Azure credential chain — uses {@link com.azure.identity.DefaultAzureCredential},
     *       which resolves: env vars → workload identity → managed identity → Azure CLI → etc.</li>
     * </ol>
     */
    private DataLakeServiceClient buildServiceClient() {
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder()
                .endpoint(serviceEndpoint);

        if (accountKey != null && !accountKey.isEmpty()) {
            builder.credential(new StorageSharedKeyCredential(accountName, accountKey));
        } else if (isServicePrincipalConfigured()) {
            TokenCredential sp = new ClientSecretCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .build();
            builder.credential(sp);
        } else {
            // DefaultAzureCredential covers: AZURE_* env vars, workload identity,
            // managed identity (system + user-assigned), Azure CLI, Azure Developer CLI,
            // Azure PowerShell — in that order.
            builder.credential(new DefaultAzureCredentialBuilder().build());
        }

        return builder.buildClient();
    }

    private boolean isServicePrincipalConfigured() {
        return tenantId != null && !tenantId.isEmpty()
                && clientId != null && !clientId.isEmpty()
                && clientSecret != null && !clientSecret.isEmpty();
    }

    private String describeAuth() {
        if (accountKey != null && !accountKey.isEmpty())   return "accountKey";
        if (isServicePrincipalConfigured())                return "servicePrincipal(clientId=" + clientId + ")";
        return "defaultAzureCredential";
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private boolean isStreamable() {
        String fmt = options.getSinkFileFormat();
        return fmt == null
                || fmt.isEmpty()
                || FileFormats.CSV.getType().equalsIgnoreCase(fmt);
    }

    /**
     * Builds the final blob path by composing three optional parts in order:
     * <pre>{base}[_{taskId}][.{compression}].{ext}</pre>
     *
     * Examples for {@code output.parquet}, SNAPPY, 4 jobs:
     * <ul>
     *   <li>taskId=0 → {@code output_0.snappy.parquet}</li>
     *   <li>jobs=1   → {@code output.snappy.parquet}</li>
     * </ul>
     */
    private String resolveTaskPath(String path, int taskId) {
        int dot = path.lastIndexOf('.');
        String base = dot >= 0 ? path.substring(0, dot) : path;
        String ext  = dot >= 0 ? path.substring(dot)    : "";  // e.g. ".parquet"

        if (options.getJobs() > 1) {
            base = base + "_" + taskId;
        }

        String compressionSuffix = parquetCompressionSuffix();
        if (!compressionSuffix.isEmpty()) {
            base = base + "." + compressionSuffix;
        }

        return base + ext;
    }

    /**
     * Returns the file-extension token for the configured Parquet compression,
     * or an empty string for non-Parquet formats or UNCOMPRESSED.
     */
    private String parquetCompressionSuffix() {
        if (!FileFormats.PARQUET.getType().equalsIgnoreCase(options.getSinkFileFormat())) {
            return "";
        }
        Properties props = options.getSinkConnectionParams();
        String codec = (props != null)
                ? props.getProperty("parquet.compression", "SNAPPY").toUpperCase()
                : "SNAPPY";
        switch (codec) {
            case "SNAPPY":       return "snappy";
            case "GZIP":         return "gz";
            case "ZSTD":         return "zstd";
            case "LZ4":
            case "LZ4_RAW":      return "lz4";
            case "BROTLI":       return "br";
            case "LZO":          return "lzo";
            case "UNCOMPRESSED": return "";
            default:             return codec.toLowerCase();
        }
    }

    private static String statsFilePath(String dataFilePath) {
        int slash = dataFilePath.lastIndexOf('/');
        return slash >= 0
                ? dataFilePath.substring(0, slash + 1) + STATS_FILE_NAME
                : STATS_FILE_NAME;
    }

    private DataLakeFileClient buildFileClient(String targetPath) {
        DataLakeFileSystemClient fsClient = buildServiceClient().getFileSystemClient(fileSystemName);
        return fsClient.getFileClient(targetPath);
    }

    // -------------------------------------------------------------------------
    // Unused SQL overrides
    // -------------------------------------------------------------------------

    @Override
    protected void createStagingTable() {
    }

    @Override
    protected void mergeStagingTable() {
    }

    @Override
    protected String mapJdbcTypeToNativeDDL(String columnName, int jdbcType, int precision, int scale) {
        throw new UnsupportedOperationException("ADLS Gen2 does not support SQL DDL.");
    }

    @Override
    public void preSourceTasks() {
    }

    @Override
    public void postSourceTasks() {
    }

    @Override
    public void cleanUp() throws Exception {
        if (fileManager != null) {
            fileManager.cleanUp();
        }
    }

    // -------------------------------------------------------------------------
    // Stats record
    // -------------------------------------------------------------------------

    private static class TaskStats {
        final String filePath;
        final int rows;
        final long durationMs;

        TaskStats(String filePath, int rows, long durationMs) {
            this.filePath = filePath;
            this.rows = rows;
            this.durationMs = durationMs;
        }
    }
}

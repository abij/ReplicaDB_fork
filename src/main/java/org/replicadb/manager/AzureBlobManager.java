package org.replicadb.manager;

import com.azure.core.credential.TokenCredential;
import com.azure.core.util.BinaryData;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
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
 * Sink manager for Azure Blob Storage (flat-namespace storage accounts).
 *
 * <p><b>Connection string format:</b>
 * <pre>https://accountname.blob.core.windows.net/container/path/to/output.parquet</pre>
 *
 * <h3>Authentication — resolved in this order</h3>
 * <ol>
 *   <li><b>Account key</b> — set {@code accountKey} in sink connection params.</li>
 *   <li><b>Service principal</b> — set {@code tenantId}, {@code clientId}, {@code clientSecret}.</li>
 *   <li><b>Default Azure credential chain</b> — no params needed.</li>
 * </ol>
 *
 * <h3>Write strategy by format</h3>
 * <ul>
 *   <li><b>CSV</b> — streamed via {@code BlockBlobClient.getBlobOutputStream()}.</li>
 *   <li><b>Parquet / ORC</b> — written to a local temp file, then uploaded via {@code uploadFromFile()}.</li>
 * </ul>
 */
public class AzureBlobManager extends SqlManager {

    private static final Logger LOG = LogManager.getLogger(AzureBlobManager.class.getName());
    private static final String STATS_FILE_NAME = "_replicadb_stats.json";
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private static final ConcurrentHashMap<Integer, TaskStats> taskStats = new ConcurrentHashMap<>();

    private String accountName;
    private String containerName;
    private String serviceEndpoint;
    private String blobPath;
    private String statsFilePathOverride;

    private String accountKey;
    private String tenantId;
    private String clientId;
    private String clientSecret;
    private String serviceVersion;   // optional: pin Blob API version (e.g. "V2024_08_04" for Azurite)

    private final FileManager fileManager;

    public AzureBlobManager(ToolOptions opts, DataSourceType dsType) {
        super(opts);
        this.dsType = dsType;
        loadBlobProperties();
        this.fileManager = new FileManagerFactory().accept(opts, dsType);
    }

    private void loadBlobProperties() {
        Properties props = dsType == DataSourceType.SOURCE
                ? options.getSourceConnectionParams()
                : options.getSinkConnectionParams();
        if (props == null) props = new Properties();

        this.accountKey     = props.getProperty("accountKey");
        this.tenantId       = props.getProperty("tenantId");
        this.clientId       = props.getProperty("clientId");
        this.clientSecret   = props.getProperty("clientSecret");
        this.serviceVersion = props.getProperty("serviceVersion");

        try {
            // https://accountname.blob.core.windows.net/container/path/to/blob
            URI uri = new URI(options.getSinkConnect());
            String host = uri.getHost();
            this.accountName     = host.substring(0, host.indexOf('.'));
            this.serviceEndpoint = "https://" + host;

            String endpointOverride = props.getProperty("endpoint");
            if (endpointOverride != null && !endpointOverride.isEmpty()) {
                this.serviceEndpoint = endpointOverride;
            }

            String uriPath = uri.getPath();
            String stripped = uriPath.startsWith("/") ? uriPath.substring(1) : uriPath;
            int slash = stripped.indexOf('/');
            String derivedPath;
            if (slash < 0) {
                this.containerName = stripped;
                derivedPath = "";
            } else {
                this.containerName = stripped.substring(0, slash);
                derivedPath = stripped.substring(slash + 1);
            }

            String keyFileNameProp = props.getProperty("keyFileName");
            this.blobPath = (keyFileNameProp != null && !keyFileNameProp.isEmpty())
                    ? keyFileNameProp
                    : derivedPath;

            String statsFile = options.getSinkStatsFile();
            if (statsFile == null || statsFile.isEmpty()) statsFile = props.getProperty("statsFile");
            this.statsFilePathOverride = (statsFile != null && !statsFile.isEmpty()) ? statsFile : null;

        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid Azure Blob Storage connection string. Expected: https://account.blob.core.windows.net/container/path", e);
        }

        LOG.debug("Azure Blob -- endpoint: {}, container: {}, path: {}, auth: {}",
                serviceEndpoint, containerName, blobPath, describeAuth());
    }

    @Override
    public Future<Integer> preSinkTasks(ExecutorService executor) throws Exception {
        validateSinkPath();
        taskStats.clear();
        return null;
    }

    private void validateSinkPath() {
        if (blobPath == null || blobPath.isEmpty()) {
            throw new IllegalArgumentException(
                    "Azure Blob sink path is missing. Specify a blob path in the URI, " +
                    "e.g. https://account.blob.core.windows.net/container/path/to/output.parquet");
        }

        BlobContainerClient containerClient = buildServiceClient().getBlobContainerClient(containerName);
        int jobs = options.getJobs();
        for (int taskId = 0; taskId < jobs; taskId++) {
            String targetPath = resolveTaskPath(blobPath, taskId);
            if (containerClient.getBlobClient(targetPath).exists()) {
                throw new IllegalStateException(
                        "Azure Blob sink file already exists: " + targetPath +
                        ". Delete it first or choose a different destination path.");
            }
        }
    }

    @Override
    public void postSinkTasks() throws Exception {
        if (taskStats.isEmpty()) {
            LOG.debug("No task stats recorded - skipping stats file");
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
                .collect(java.util.stream.Collectors.toList())) {
            TaskStats s = entry.getValue();
            ObjectNode t = tasksNode.addObject();
            t.put("taskId", entry.getKey());
            t.put("file", s.blobPath);
            t.put("rows", s.rows);
            t.put("durationMs", s.durationMs);
            totalRows += s.rows;
        }

        root.put("totalRows", totalRows);

        byte[] json = MAPPER.writeValueAsBytes(root);
        String statsPath = statsFilePathOverride != null ? statsFilePathOverride : statsFilePath(blobPath);

        LOG.info("Writing replication stats to Azure Blob: {}", statsPath);
        buildServiceClient()
                .getBlobContainerClient(containerName)
                .getBlobClient(statsPath)
                .upload(BinaryData.fromBytes(json), true);
    }

    @Override
    public String getDriverClass() {
        return JdbcDrivers.AZBLOB.getDriverClass();
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
        String targetPath = resolveTaskPath(blobPath, taskId);
        LOG.info("Writing to Azure Blob: container={}, path={}", containerName, targetPath);

        BlobContainerClient containerClient = buildServiceClient().getBlobContainerClient(containerName);
        long start = System.currentTimeMillis();
        int processedRows;

        if (isStreamable()) {
            BlockBlobClient blockBlob = containerClient.getBlobClient(targetPath).getBlockBlobClient();
            try (OutputStream os = blockBlob.getBlobOutputStream(true)) {
                processedRows = fileManager.writeData(os, resultSet, taskId, null);
            }
        } else {
            processedRows = fileManager.writeData(OutputStream.nullOutputStream(), resultSet, taskId, null);
            String tempFilePath = FileManager.getTempFilePath(taskId);
            LOG.debug("Uploading temp file to Azure Blob: {}", tempFilePath);
            containerClient.getBlobClient(targetPath).uploadFromFile(tempFilePath, true);
        }

        long durationMs = System.currentTimeMillis() - start;
        taskStats.put(taskId, new TaskStats(targetPath, processedRows, durationMs));

        LOG.info("Uploaded {} rows -> https://{}.blob.core.windows.net/{}/{} ({}ms)",
                processedRows, accountName, containerName, targetPath, durationMs);
        return processedRows;
    }

    private BlobServiceClient buildServiceClient() {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .endpoint(serviceEndpoint);

        if (serviceVersion != null && !serviceVersion.isEmpty()) {
            try {
                builder.serviceVersion(BlobServiceVersion.valueOf(serviceVersion));
            } catch (IllegalArgumentException e) {
                LOG.warn("Unknown serviceVersion '{}', using default", serviceVersion);
            }
        }

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

    private boolean isStreamable() {
        String fmt = options.getSinkFileFormat();
        return fmt == null
                || fmt.isEmpty()
                || FileFormats.CSV.getType().equalsIgnoreCase(fmt);
    }

    private String resolveTaskPath(String path, int taskId) {
        int slash = path.lastIndexOf('/');
        String dir      = slash >= 0 ? path.substring(0, slash + 1) : "";
        String filename = slash >= 0 ? path.substring(slash + 1)    : path;

        if (filename.isEmpty()) {
            String fmt = options.getSinkFileFormat();
            filename = "output." + (fmt != null && !fmt.isEmpty() ? fmt.toLowerCase() : "parquet");
        }

        int dot = filename.lastIndexOf('.');
        String base = dot >= 0 ? filename.substring(0, dot) : filename;
        String ext  = dot >= 0 ? filename.substring(dot)    : "";

        if (options.getJobs() > 1) {
            base = base + "_" + taskId;
        }

        String compressionSuffix = parquetCompressionSuffix();
        if (!compressionSuffix.isEmpty()) {
            base = base + "." + compressionSuffix;
        }

        return dir + base + ext;
    }

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

    @Override
    protected void createStagingTable() {
    }

    @Override
    protected void mergeStagingTable() {
    }

    @Override
    protected String mapJdbcTypeToNativeDDL(String columnName, int jdbcType, int precision, int scale) {
        throw new UnsupportedOperationException("Azure Blob Storage does not support SQL DDL.");
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

    private static class TaskStats {
        final String blobPath;
        final int rows;
        final long durationMs;

        TaskStats(String blobPath, int rows, long durationMs) {
            this.blobPath = blobPath;
            this.rows = rows;
            this.durationMs = durationMs;
        }
    }
}

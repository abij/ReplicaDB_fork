package org.replicadb.config;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Singleton Testcontainers wrapper for Azurite — Microsoft's Azure Storage emulator.
 *
 * <p>Azurite serves ADLS Gen2 (DFS) operations on the same port as Blob storage (10000).
 * Use {@link #getDfsEndpoint()} as the {@code endpoint} connection param in
 * {@code ADLSGen2Manager} to override the default service URL derived from the URI.
 */
public class ReplicadbAzuriteContainer extends GenericContainer<ReplicadbAzuriteContainer> {

    private static final Logger LOG = LogManager.getLogger(ReplicadbAzuriteContainer.class);

    private static final DockerImageName IMAGE =
            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.33.0");

    // Well-known Azurite development credentials — safe to commit, not real secrets
    public static final String ACCOUNT_NAME = "devstoreaccount1";
    public static final String ACCOUNT_KEY  =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    public static final String TEST_FILESYSTEM = "replicadb-test";

    /** Blob / DFS port — Azurite serves both on the same port. */
    private static final int BLOB_PORT = 10000;

    private static ReplicadbAzuriteContainer container;

    private ReplicadbAzuriteContainer() {
        super(IMAGE);
        withExposedPorts(BLOB_PORT);
        withReuse(true);
    }

    public static ReplicadbAzuriteContainer getInstance() {
        if (container == null) {
            container = new ReplicadbAzuriteContainer();
            container.start();
        }
        return container;
    }

    @Override
    public void start() {
        super.start();
        LOG.info("Azurite started on port {}", getMappedPort(BLOB_PORT));
        // createIfNotExists() is idempotent — safe when container is reused across runs
        buildServiceClient().getFileSystemClient(TEST_FILESYSTEM).createIfNotExists();
        LOG.info("Azurite filesystem '{}' ready", TEST_FILESYSTEM);
    }

    @Override
    public void stop() {
        // JVM handles shutdown
    }

    /**
     * Returns the DFS endpoint for use as the {@code endpoint} connection param.
     * Format matches what {@code DataLakeServiceClientBuilder.endpoint()} expects for Azurite.
     */
    public String getDfsEndpoint() {
        return "http://127.0.0.1:" + getMappedPort(BLOB_PORT) + "/" + ACCOUNT_NAME;
    }

    /** Builds a service client pointed at this Azurite instance. */
    public DataLakeServiceClient buildServiceClient() {
        return new DataLakeServiceClientBuilder()
                .endpoint(getDfsEndpoint())
                .credential(new StorageSharedKeyCredential(ACCOUNT_NAME, ACCOUNT_KEY))
                .buildClient();
    }

    /** Convenience: returns the filesystem client for the test filesystem. */
    public DataLakeFileSystemClient getTestFilesystemClient() {
        return buildServiceClient().getFileSystemClient(TEST_FILESYSTEM);
    }
}

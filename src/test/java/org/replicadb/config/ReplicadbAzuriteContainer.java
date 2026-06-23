package org.replicadb.config;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobServiceVersion;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceVersion;
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

    /** API version supported by Azurite 3.33.0. Set as serviceVersion in test params. */
    public static final String COMPATIBLE_SERVICE_VERSION = "V2021_12_02";

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
            ReplicadbAzuriteContainer c = new ReplicadbAzuriteContainer();
            c.start();         // if start() throws, c is never assigned — next call retries
            container = c;
        }
        return container;
    }

    @Override
    public void start() {
        super.start();
        LOG.info("Azurite started on port {}", getMappedPort(BLOB_PORT));
        // Create the filesystem via Blob API — the DFS filesystem is backed by a Blob container.
        // Using Blob API here avoids DataLakeServiceVersion vs internal-Blob-version mismatch
        // that would cause Azurite 3.33.0 to reject the DFS createIfNotExists call.
        buildBlobServiceClient().getBlobContainerClient(TEST_FILESYSTEM).createIfNotExists();
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
                .serviceVersion(DataLakeServiceVersion.valueOf(COMPATIBLE_SERVICE_VERSION))
                .buildClient();
    }

    /** Convenience: returns the filesystem client for the test filesystem. */
    public DataLakeFileSystemClient getTestFilesystemClient() {
        return buildServiceClient().getFileSystemClient(TEST_FILESYSTEM);
    }

    /**
     * Returns the Blob service endpoint for Azurite.
     * For Azurite, Blob and DFS endpoints are the same port.
     */
    public String getBlobEndpoint() {
        return "http://127.0.0.1:" + getMappedPort(BLOB_PORT) + "/" + ACCOUNT_NAME;
    }

    /** Convenience: returns the blob container client for the test container. */
    public BlobContainerClient getBlobContainerClient() {
        return buildBlobServiceClient().getBlobContainerClient(TEST_FILESYSTEM);
    }

    private BlobServiceClient buildBlobServiceClient() {
        return new BlobServiceClientBuilder()
                .endpoint(getBlobEndpoint())
                .credential(new StorageSharedKeyCredential(ACCOUNT_NAME, ACCOUNT_KEY))
                .serviceVersion(BlobServiceVersion.valueOf(COMPATIBLE_SERVICE_VERSION))
                .buildClient();
    }
}

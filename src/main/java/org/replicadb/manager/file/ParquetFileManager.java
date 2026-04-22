package org.replicadb.manager.file;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.replicadb.cli.ToolOptions;
import org.replicadb.manager.DataSourceType;
import org.replicadb.manager.util.BandwidthThrottling;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

import static java.sql.Types.*;
import static org.replicadb.manager.LocalFileManager.getFileFromPathString;

/**
 * Manages reading and writing Parquet files.
 * Write path: ResultSet → local temp file (via ParquetWriter) → OutputStream.
 * Reading Parquet as source is not yet supported.
 */
public class ParquetFileManager extends FileManager {

    private static final Logger LOG = LogManager.getLogger(ParquetFileManager.class);

    public ParquetFileManager(ToolOptions opts, DataSourceType dsType) {
        super(opts, dsType);
    }

    @Override
    public void init() throws SQLException {
        // Source reading not yet implemented
    }

    @Override
    public ResultSet readData() {
        throw new UnsupportedOperationException("Parquet source is not yet supported");
    }

    @Override
    public int writeData(OutputStream out, ResultSet resultSet, int taskId, File tempFile) throws IOException, SQLException {
        if (tempFile == null) tempFile = createTemporalFile(taskId);

        Configuration conf = new Configuration();
        // Disable Hadoop CRC files — not needed for temp files
        conf.set("dfs.checksum.type", "NULL");

        Properties props = dsType == DataSourceType.SOURCE
                ? options.getSourceConnectionParams()
                : options.getSinkConnectionParams();
        CompressionCodecName codec = resolveCompression(props.getProperty("parquet.compression"));

        MessageType schema = messageTypeFromResultSet(resultSet);
        LOG.info("Sink Parquet schema: {}", schema);

        Path path = new Path(tempFile.toURI());
        int processedRows = 0;

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
                .withConf(conf)
                .withType(schema)
                .withCompressionCodec(codec)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .build()) {

            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

            if (resultSet.next()) {
                BandwidthThrottling bt = new BandwidthThrottling(options.getBandwidthThrottling(), options.getFetchSize(), resultSet);
                do {
                    bt.acquiere();
                    processedRows++;
                    Group group = groupFactory.newGroup();
                    fillGroup(group, resultSet, schema);
                    writer.write(group);
                } while (resultSet.next());
            }
        }

        LOG.debug("Parquet file generated at {}", tempFile.getPath());
        IOUtils.copy(FileUtils.openInputStream(tempFile), out);
        return processedRows;
    }

    private File createTemporalFile(int taskId) throws IOException {
        java.nio.file.Path temp = Files.createTempFile("repdb", ".parquet");
        LOG.info("Temporal Parquet file: {}", temp.toAbsolutePath());
        setTempFilePath(taskId, temp.toAbsolutePath().toString());
        return temp.toFile();
    }

    /**
     * Build a Parquet MessageType from JDBC ResultSetMetaData.
     */
    private static MessageType messageTypeFromResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 1; i <= columnCount; i++) {
            String name = rsmd.getColumnName(i);
            int precision = rsmd.getPrecision(i);
            int scale = rsmd.getScale(i);

            switch (rsmd.getColumnType(i)) {
                case CHAR:
                case VARCHAR:
                case LONGVARCHAR:
                case CLOB:
                case SQLXML:
                    builder.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    builder.optional(PrimitiveTypeName.INT32).named(name);
                    break;
                case BIGINT:
                    builder.optional(PrimitiveTypeName.INT64).named(name);
                    break;
                case REAL:
                    // SQL REAL = single-precision 32-bit (e.g. MySQL FLOAT → java.sql.Types.REAL)
                    builder.optional(PrimitiveTypeName.FLOAT).named(name);
                    break;
                case FLOAT:
                    // SQL FLOAT = approximate numeric, always double-precision in practice
                    // (Oracle FLOAT, PostgreSQL FLOAT8, SQL Server FLOAT). Map to DOUBLE to
                    // avoid silent precision loss from narrowing to 32-bit.
                case DOUBLE:
                    builder.optional(PrimitiveTypeName.DOUBLE).named(name);
                    break;
                case BOOLEAN:
                case BIT:
                    builder.optional(PrimitiveTypeName.BOOLEAN).named(name);
                    break;
                case NUMERIC:
                case DECIMAL:
                    // Oracle NUMBER without explicit precision returns precision=0, scale=-127.
                    // Parquet DECIMAL requires precision >= 1 and scale >= 0.
                    int decPrecision = precision > 0 ? precision : 38;
                    int decScale     = Math.max(0, scale);
                    builder.optional(PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.decimalType(decScale, decPrecision))
                            .named(name);
                    break;
                case DATE:
                    builder.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named(name);
                    break;
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIMEZONE:
                case -101: // Oracle TIMESTAMP WITH TIME ZONE
                case -102: // Oracle TIMESTAMP WITH LOCAL TIME ZONE
                    builder.optional(PrimitiveTypeName.INT64)
                            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                            .named(name);
                    break;
                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case BLOB:
                    builder.optional(PrimitiveTypeName.BINARY).named(name);
                    break;
                case ARRAY:
                    // Arrays serialized as their string representation
                    builder.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
                    break;
                default:
                    builder.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
                    break;
            }
        }
        return builder.named("record");
    }

    /**
     * Populate a Parquet Group from the current ResultSet row.
     *
     * <p>Dispatches on the <em>Parquet schema type</em> (not the JDBC type) so that all
     * parallel partitions write consistently even when a driver (e.g. SQLite) reports
     * different JDBC type codes for the same logical column across partitions.
     */
    private static void fillGroup(Group group, ResultSet resultSet, MessageType schema) throws SQLException, IOException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String name = rsmd.getColumnName(i);
            Object value = resultSet.getObject(i);
            if (value == null) continue; // optional fields — just omit

            org.apache.parquet.schema.Type fieldType = schema.getType(name);
            PrimitiveTypeName primitive = fieldType.asPrimitiveType().getPrimitiveTypeName();
            LogicalTypeAnnotation logical = fieldType.getLogicalTypeAnnotation();

            switch (primitive) {
                case INT32:
                    if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        // DATE — days since Unix epoch
                        try {
                            Date dateVal = resultSet.getDate(i);
                            if (dateVal != null) group.add(name, (int) dateVal.toLocalDate().toEpochDay());
                        } catch (SQLException ignored) {
                            // Some drivers (e.g. SQLite) return dates as ISO strings
                            group.add(name, (int) java.time.LocalDate.parse(value.toString()).toEpochDay());
                        }
                    } else {
                        group.add(name, ((Number) value).intValue());
                    }
                    break;
                case INT64:
                    if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        Timestamp ts = resultSet.getTimestamp(i);
                        if (ts != null) group.add(name, ts.getTime());
                    } else {
                        group.add(name, ((Number) value).longValue());
                    }
                    break;
                case FLOAT:
                    group.add(name, ((Number) value).floatValue());
                    break;
                case DOUBLE:
                    group.add(name, ((Number) value).doubleValue());
                    break;
                case BOOLEAN:
                    group.add(name, resultSet.getBoolean(i));
                    break;
                case BINARY:
                    if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                        // NUMERIC / DECIMAL — unscaled bytes, aligned to schema scale
                        BigDecimal bd = resultSet.getBigDecimal(i);
                        if (bd != null) {
                            int schemaScale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logical).getScale();
                            bd = bd.setScale(schemaScale, java.math.RoundingMode.HALF_UP);
                            group.add(name, Binary.fromConstantByteArray(bd.unscaledValue().toByteArray()));
                        }
                    } else if (logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                        // STRING — covers CHAR/VARCHAR/CLOB/SQLXML/ARRAY and the default fallback
                        int jdbcType = rsmd.getColumnType(i);
                        if (jdbcType == CLOB) {
                            Clob clob = resultSet.getClob(i);
                            group.add(name, Binary.fromString(clob.getSubString(1, (int) clob.length())));
                            clob.free();
                        } else if (jdbcType == SQLXML) {
                            SQLXML sqlxml = resultSet.getSQLXML(i);
                            group.add(name, Binary.fromString(sqlxml.getString()));
                            sqlxml.free();
                        } else if (jdbcType == ARRAY) {
                            Array arr = resultSet.getArray(i);
                            group.add(name, Binary.fromString(java.util.Arrays.deepToString((Object[]) arr.getArray())));
                            arr.free();
                        } else {
                            group.add(name, Binary.fromString(value.toString()));
                        }
                    } else {
                        // Raw binary — BINARY/VARBINARY/BLOB
                        if (rsmd.getColumnType(i) == BLOB) {
                            Blob blob = resultSet.getBlob(i);
                            group.add(name, Binary.fromConstantByteArray(IOUtils.toByteArray(blob.getBinaryStream())));
                            blob.free();
                        } else {
                            byte[] bytes = resultSet.getBytes(i);
                            if (bytes != null) group.add(name, Binary.fromConstantByteArray(bytes));
                        }
                    }
                    break;
                default:
                    group.add(name, Binary.fromString(value.toString()));
                    break;
            }
        }
    }

    private static CompressionCodecName resolveCompression(String codec) {
        if (codec == null || codec.isEmpty()) codec = "SNAPPY";
        try {
            return CompressionCodecName.valueOf(codec.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOG.warn("Unknown Parquet compression '{}', defaulting to SNAPPY", codec);
            return CompressionCodecName.SNAPPY;
        }
    }

    @Override
    public void mergeFiles() throws IOException, URISyntaxException {
        // Parquet has no native merge API. Multi-job (--jobs > 1) writes produce one file
        // per task already named with _taskId suffix (handled by the storage manager).
        // For local file sinks with --jobs=1 this is a no-op rename.
        if (getTempFilePathSize() == 0) return;

        File finalFile = getFileFromPathString(options.getSinkConnect());
        File firstTemp = getFileFromPathString(getTempFilePath(0));

        Files.move(firstTemp.toPath(), finalFile.toPath(),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        LOG.info("Parquet file written to {}", finalFile.getPath());

        if (getTempFilePathSize() > 1) {
            LOG.warn("Parquet merge of {} files is not supported; only the first temp file was kept. Use --jobs=1 for local Parquet output.", getTempFilePathSize());
        }
    }

    @Override
    public void cleanUp() {
        for (Map.Entry<Integer, String> entry : getTempFilesPath().entrySet()) {
            try {
                File f = getFileFromPathString(entry.getValue());
                if (f != null && f.exists()) {
                    f.delete();
                    LOG.debug("Removed temp Parquet file {}", f.getPath());
                }
            } catch (MalformedURLException | URISyntaxException e) {
                LOG.error("Error cleaning up temp file: {}", entry.getValue(), e);
            }
        }
    }
}

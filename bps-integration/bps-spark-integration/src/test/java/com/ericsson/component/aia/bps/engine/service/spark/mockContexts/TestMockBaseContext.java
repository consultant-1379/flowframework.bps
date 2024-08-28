package com.ericsson.component.aia.bps.engine.service.spark.mockContexts;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.EXPECTED_CSV_DATA_SET;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.ROOT_BASE_FOLDER;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.TABLE_HEADER;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.skyscreamer.jsonassert.JSONAssert;

import com.ericsson.component.aia.bps.core.common.DataFormat;
import com.ericsson.component.aia.bps.engine.service.spark.common.TestUtil;
import com.google.gson.JsonSyntaxException;

import parquet.example.data.Group;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

/**
 * BaseMockContext is the parent base class for all mock context objects and it holds common code useful while running testcase.
 */
@Ignore
public abstract class TestMockBaseContext {

    /** The Constant CSV_DELIMITER. */
    public static final String CSV_DELIMITER = ",";

    /** The Constant fileFilter. */
    protected static final FileFilter fileFilter = new FileFilter() {
        @Override
        public boolean accept(final File file) {
            return !file.isHidden() && (!file.getName().toString().endsWith(".crc") && !StringUtils.contains(file.getName().toString(), "_metadata")
                    && !file.getName().toString().endsWith("_SUCCESS"));
        }
    };

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestMockBaseContext.class);

    /** The tmp dir. */
    protected Path tmpDir;

    /** The input data format. */
    protected String inputDataFormat;

    /** The output data format. */
    protected String outputDataFormat;

    /**
     * Instantiates a new base mock context.
     */
    public TestMockBaseContext() {

    }

    /**
     * Instantiates a new base mock context.
     *
     * @param className
     *            the class name
     */
    public TestMockBaseContext(final String className) {

        /*
         * This configuration should be set incase of windows System.setProperty("hadoop.home.dir", "C:\\aia\\components\\hadoop-bin");
         */
        final String dir = ROOT_BASE_FOLDER + className;
        final File fileDir = new File(dir);

        if (fileDir.exists()) {
            fileDir.delete();
        }

        fileDir.mkdir();
        tmpDir = fileDir.toPath();
    }

    /**
     * Input configurations for a input source as defined in flow xml.
     *
     * @return the map
     */
    public abstract Map<String, String> inputConfigurations();

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     */
    public abstract Map<String, String> outputConfigurations();

    /**
     * Validates test case scenario.
     */
    public abstract void validate();

    /**
     * StepConfigurations for a Job as defined in flow xml.
     *
     * @return the map
     */
    public Map<String, String> stepConfigurations() {

        final Map<String, String> stepMap = new HashMap<String, String>();
        stepMap.put("uri", "spark-batch://sales-analysis");
        stepMap.put("sql", "SELECT TRANSACTION_DATE,PRODUCT,PRICE,PAYMENT_TYPE,NAME,"
                + "CITY,STATE,COUNTRY,ACCOUNT_CREATED,LAST_LOGIN,LATITUDE,LONGITUDE FROM sales");

        for (final Map.Entry<String, String> entry : getConfigMap().entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
            stepMap.put(entry.getKey(), entry.getValue());
        }

        return stepMap;
    }

    /**
     * Clean up operation for junit test cases.
     */
    public void cleanUp() {

        deleteFolder(tmpDir.toFile());

        for (final Map.Entry<String, String> entry : getConfigMap().entrySet()) {
            deleteFolder(new File(entry.getValue()));
        }
    }

    /**
     * Delete folder.
     *
     * @param file
     *            the file
     */
    protected void deleteFolder(final File file) {
        try {
            FileDeleteStrategy.FORCE.delete(file);
        } catch (final IOException e) {
            LOGGER.debug("CleanUp, IOException", e);
        }
    }

    /**
     * Gets the filtered actual files.
     *
     * @param outputDirectory
     *            the output directory
     * @return the filtered actual files
     */
    protected File[] getFilteredActualFiles(final String outputDirectory) {
        final File[] actual = new File(outputDirectory).listFiles(fileFilter);

        Arrays.sort(actual);

        return actual;
    }

    /**
     * Validates expected & actual file output data.
     *
     * @param outputDirectory
     *            the outputDirectory
     * @param expectedOpPath
     *            the expected output path
     * @param dataFormat
     *            the data format
     * @throws Exception
     *             the exception
     * @throws JsonSyntaxException
     *             the json syntax exception
     */

    public void validateFileOutput(final String outputDirectory, final String expectedOpPath, final String dataFormat) throws Exception {

        final File expectedFile;

        final File[] actual = getFilteredActualFiles(outputDirectory);
        final File actualFile;

        if (DataFormat.PARQUET == DataFormat.valueOf(dataFormat)) {

            final File csvOutputFile = new File(outputDirectory + SEPARATOR + "converted_csv_" + System.currentTimeMillis());

            for (final File tmpFile : actual) {
                convertParquetToCSV(tmpFile, csvOutputFile);
            }

            actualFile = csvOutputFile;
            expectedFile = new File(EXPECTED_CSV_DATA_SET);
        } else {
            actualFile = new File(outputDirectory + System.currentTimeMillis());
            expectedFile = new File(expectedOpPath);
            TestUtil.joinFiles(actualFile, actual);
        }

        if (DataFormat.JSON == DataFormat.valueOf(dataFormat)) {

            final String expectedStr = FileUtils.readFileToString(expectedFile);
            final String actualStr = FileUtils.readFileToString(actualFile);

            JSONAssert.assertEquals(expectedStr, actualStr, false);

        } else {
            containsExactText(expectedFile, actualFile);

        }
    }

    /**
     * Validate Validates expected & actual DB output data.
     *
     * @param conn
     *            the connection
     * @param dbName
     *            the database name
     * @param query
     *            the query
     * @param expectedFilePath
     *            the expected file path
     */
    public void validateDBOutput(final Connection conn, final String dbName, String query, final String expectedFilePath) {

        final String JDBC_OP = tmpDir.toAbsolutePath() + SEPARATOR + dbName + "_JDBC_OP.csv";
        query = query.replace("JDBC_OP", JDBC_OP);
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            conn.close();
        } catch (final Exception e) {
            Assert.fail(e.getMessage());
        }

        final File expectedFile = new File(expectedFilePath);
        final File actualFile = new File(JDBC_OP);

        try {
            containsExactText(expectedFile, actualFile);
        } catch (final IOException e) {

            try {
                LOGGER.info("FileUtils.readLines(actualFile)----" + FileUtils.readLines(actualFile));
                LOGGER.info("FileUtils.readLines(expectedFile)----" + FileUtils.readLines(expectedFile));
            } catch (final IOException e1) {
                LOGGER.debug("validateDBOutput, IOException", e);
            }

            Assert.fail(e.getMessage());
        }
    }

    /**
     * Gets the config map.
     *
     * @return the config map
     */
    private Map<String, String> getConfigMap() {
        final Map<String, String> sparkConfigMap = new HashMap<>();
        final String tmpFolder = tmpDir.toAbsolutePath() + SEPARATOR;
        sparkConfigMap.put("spark.local.dir", tmpFolder + "spark_local_dir");
        sparkConfigMap.put("hive.exec.dynamic.partition.mode", "nonstrict");
        sparkConfigMap.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConfigMap.put("spark.externalBlockStore.url", tmpFolder + "spark.externalBlockStore.url");
        sparkConfigMap.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        sparkConfigMap.put("hive.metastore.warehouse.dir", tmpFolder + "hive");
        sparkConfigMap.put("spark.externalBlockStore.baseDir", tmpFolder + "spark.externalBlockStore.baseDir");
        sparkConfigMap.put("hive.exec.scratchdir", tmpFolder + "hive.exec.scratchdir");
        sparkConfigMap.put("hive.downloaded.resources.dir", tmpDir + SEPARATOR + "hive.resources.dir_" + System.currentTimeMillis());
        sparkConfigMap.put("hive.querylog.location", tmpFolder + "querylog.location");
        sparkConfigMap.put("hive.exec.local.scratchdir", tmpFolder + "hive.exec.local.scratchdir");
        sparkConfigMap.put("hive.downloaded.resources.dir", tmpFolder + "hive.downloaded.resources.dir");
        sparkConfigMap.put("hive.metadata.export.location", tmpFolder + "hive.metadata.export.location");
        sparkConfigMap.put("hive.metastore.metadb.dir", tmpFolder + "hive.metastore.metadb.dir");
        sparkConfigMap.put("hive.merge.sparkfiles", "true");

        return sparkConfigMap;
    }

    /**
     * Removes the header if exits.
     *
     * @param actualFile
     *            the actual file
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List<String> removeHeaderIfExits(final File actualFile) throws IOException {

        final List<String> actualLines = FileUtils.readLines(actualFile);

        final List<String> removeHeaderlist = new ArrayList<String>();

        for (int i = 0; i < 2; ++i) {

            if (actualLines.get(i).contains(TABLE_HEADER)) {
                removeHeaderlist.add(actualLines.get(i));
            }
        }

        actualLines.removeAll(removeHeaderlist);

        return actualLines;
    }

    /**
     * Contains exact text.
     *
     * @param expectedFile
     *            the expected file
     * @param actualFile
     *            the actual file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void containsExactText(final File expectedFile, final File actualFile) throws IOException {

        final List<String> actualLines = removeHeaderIfExits(actualFile);

        final List<String> expectedLines = FileUtils.readLines(expectedFile);

        Collections.sort(actualLines);
        Collections.sort(expectedLines);

        Assert.assertEquals(expectedLines, actualLines);
    }

    /**
     * Gets the extension.
     *
     * @param dataFormat
     *            the data format
     * @return the extension
     */
    protected String getExtension(final String dataFormat) {
        return "." + DataFormat.valueOf(dataFormat).dataFormat;
    }

    /**
     * Gets the data format.
     *
     * @param dataFormat
     *            the data format
     * @return the data format
     */
    protected String getDataFormat(final String dataFormat) {
        return DataFormat.valueOf(dataFormat).dataFormat;
    }

    /**
     * Convert parquet to CSV.
     *
     * @param parquetFile
     *            the parquet file
     * @param csvOutputFile
     *            the csv output file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("deprecation")
    public static void convertParquetToCSV(final File parquetFile, final File csvOutputFile) throws IOException {

        LOGGER.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

        final org.apache.hadoop.fs.Path parquetFilePath = new org.apache.hadoop.fs.Path(parquetFile.toURI());
        final Configuration configuration = new Configuration(true);
        final GroupReadSupport readSupport = new GroupReadSupport();
        final ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
        final MessageType schema = readFooter.getFileMetaData().getSchema();

        readSupport.init(configuration, null, schema);
        final BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile, true));
        final ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);
        try {
            Group g = null;
            while ((g = reader.read()) != null) {
                writeGroup(w, g, schema);
            }
            reader.close();
        } finally {
            closeQuietly(w);
        }
    }

    /**
     * Write group.
     *
     * @param writer
     *            the writer
     * @param grp
     *            the grp
     * @param schema
     *            the schema
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void writeGroup(final BufferedWriter writer, final Group grp, final MessageType schema) throws IOException {
        for (int j = 0; j < schema.getFieldCount(); j++) {
            if (j > 0) {
                writer.write(CSV_DELIMITER);
            }
            final String valueToString = grp.getValueToString(j, 0);
            writer.write(valueToString);
        }
        writer.write('\n');
    }

    /**
     * Close quietly.
     *
     * @param res
     *            the res
     */
    public static void closeQuietly(final Closeable res) {
        try {
            if (res != null) {
                res.close();
            }
        } catch (final IOException ioe) {
            LOGGER.warn("Exception closing reader " + res + ": " + ioe.getMessage());
        }
    }

    /**
     * Gets the input data format.
     *
     * @return the inputDataFormat
     */
    public String getInputDataFormat() {
        return inputDataFormat;
    }

    /**
     * Sets the input data format.
     *
     * @param inputDataFormat
     *            the inputDataFormat to set
     */
    public void setInputDataFormat(final String inputDataFormat) {
        this.inputDataFormat = inputDataFormat;
    }

    /**
     * Gets the output data format.
     *
     * @return the outputDataFormat
     */
    public String getOutputDataFormat() {
        return outputDataFormat;
    }

    /**
     * Sets the output data format.
     *
     * @param outputDataFormat
     *            the outputDataFormat to set
     */
    public void setOutputDataFormat(final String outputDataFormat) {
        this.outputDataFormat = outputDataFormat;
    }

    /**
     * Gets the temporary directory.
     *
     * @return the tmpDir
     */
    public Path getTmpDir() {
        return tmpDir;
    }

}

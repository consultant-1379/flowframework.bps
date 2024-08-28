package com.ericsson.component.aia.bps.engine.service.spark.mockContexts;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.DATA_FILE_NAME;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.DEFAULT_INPUT_DATASET;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.DERBY_CLIENT_DRIVER;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.EXPECTED_OUTPUT_DIR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.HIVE_DRIVER;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.HIVE_URL;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.bps.core.common.DataFormat;
import com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants;

/**
 * The Class TestMockHiveContext.
 */
public class TestMockHiveContext extends TestMockBaseContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestMockHiveContext.class);

    /** The Constant USERNAME. */
    private static final String USERNAME = "me";

    /** The Constant PASSWORD. */
    private static final String PASSWORD = "mine";

    /** The conn. */
    private Connection conn;

    /** The derby location. */
    private String DERBY_LOCATION;

    /** The warehouse dir. */
    private String warehouse_dir;

    /** The input table folder. */
    private String INPUT_TABLE_FOLDER;

    /** The table creation status. */
    private boolean tableCreationStatus;

    /**
     * Instantiates a new test mock hive context.
     *
     * @throws Exception
     *             the exception
     */
    public TestMockHiveContext() throws Exception {

        super(TestMockHiveContext.class.getSimpleName() + System.currentTimeMillis());

        DERBY_LOCATION = "jdbc:derby:" + tmpDir.toAbsolutePath() + SEPARATOR + "metastore_db;";
        warehouse_dir = tmpDir.toAbsolutePath() + SEPARATOR + "hive" + SEPARATOR + "sales_output";

        System.setProperty("javax.jdo.option.ConnectionURL", DERBY_LOCATION + ";create=true;user=" + USERNAME + ";password=" + PASSWORD);

        //Added these configurations so that it don't create folders directly
        System.setProperty("hive.exec.local.scratchdir", tmpDir.toAbsolutePath() + SEPARATOR + "local.scratchdir");
        System.setProperty("hive.exec.scratchdir", tmpDir.toAbsolutePath() + SEPARATOR + "scratchdir");
        System.setProperty("hive.metastore.metadb.dir", tmpDir.toAbsolutePath() + SEPARATOR + "metadbdir");
        System.setProperty("hive.metastore.warehouse.dir", warehouse_dir);
        System.setProperty("hive.querylog.location", tmpDir.toAbsolutePath() + SEPARATOR + "querylog");
        System.setProperty("hive.downloaded.resources.dir", tmpDir.toAbsolutePath() + SEPARATOR + "resources.dir");

        inputDataFormat = DataFormat.TEXTFILE.name();
        outputDataFormat = DataFormat.PARQUET.name();
    }

    /**
     * Input configurations.
     *
     * @return the map
     */
    @Override
    public Map<String, String> inputConfigurations() {

        if (!tableCreationStatus) {
            try {
                createInputDataFolder();
            } catch (final IOException e) {
                Assert.fail(e.getMessage());
            }

            createHiveInputTable();

            tableCreationStatus = true;
        }

        final Map<String, String> input = new HashMap<String, String>();
        input.put("uri", "hive://sales");
        input.put("driver", HIVE_URL);
        return input;
    }

    /**
     * Output configurations.
     *
     * @return the map
     */
    @Override
    public Map<String, String> outputConfigurations() {
        final Map<String, String> opConfigs = inputConfigurations();
        opConfigs.put("uri", "hive://sales_output");
        opConfigs.put("data.format", getDataFormat(outputDataFormat));
        return opConfigs;
    };

    /**
     * Step configurations.
     *
     * @return the map
     */
    @Override
    public Map<String, String> stepConfigurations() {

        final Map<String, String> stepMap = super.stepConfigurations();
        stepMap.put("master.url", "local[*]");
        return stepMap;
    }

    /**
     * Validate.
     */
    @Override
    public void validate() {

        try {
            validateFileOutput(warehouse_dir, EXPECTED_OUTPUT_DIR + "expected_output" + getExtension(outputDataFormat), outputDataFormat);
        } catch (final Exception e) {
            LOGGER.info("validate operation failed got Exception", e);
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Creates the input data folder.
     *
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void createInputDataFolder() throws IOException {

        INPUT_TABLE_FOLDER = tmpDir + SEPARATOR + "Hive_InputData_Folder" + System.currentTimeMillis();

        final File srcFile = new File(TestConstants.INPUT_DATA_FOLDER + DATA_FILE_NAME + getExtension(DataFormat.TEXTFILE.name()));

        FileUtils.copyFileToDirectory(srcFile, new File(INPUT_TABLE_FOLDER));

    }

    /**
     * Initialize hive context.
     */
    private void initializeHiveContext() {

        try {
            Class.forName(HIVE_DRIVER);
            conn = DriverManager.getConnection("jdbc:hive2://", "", "");
            LOGGER.debug("Hive Context created");
        } catch (final ClassNotFoundException e) {
            Assert.fail(e.getMessage());
        } catch (final SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Creates the hive input table.
     */
    private void createHiveInputTable() {
        initializeHiveContext();
        Statement stmt = null;

        String columns = "";

        final Reader in;
        Iterable<CSVRecord> records = null;

        try {
            in = new FileReader(DEFAULT_INPUT_DATASET);
            records = CSVFormat.RFC4180.parse(in);
        } catch (final Exception e) {
            LOGGER.info("Unable to read INPUT_FILE, Exception", e);
            Assert.fail(e.getMessage());
        }

        for (final CSVRecord record : records) {

            final int size = record.size();

            for (int i = 0; i < size; ++i) {
                if (i == 0) {
                    columns = record.get(i) + " varchar(500) ";
                } else {
                    columns = columns + ", " + record.get(i) + " varchar(500) ";
                }
            }

            break;
        }

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("create external table IF NOT EXISTS sales( " + columns + " ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS "
                    + inputDataFormat + " location '" + INPUT_TABLE_FOLDER + "' tblproperties ('skip.header.line.count'='1')");
            LOGGER.debug("External table created");
        } catch (final Exception e) {
            LOGGER.info("Unable to create external table, Exception", e);
            Assert.fail(e.getMessage());
        }

        shutdown();

        System.setProperty("javax.jdo.option.ConnectionURL", DERBY_LOCATION);
        System.setProperty("javax.jdo.option.ConnectionDriverName", DERBY_CLIENT_DRIVER);

        System.setProperty("javax.jdo.option.ConnectionUserName", USERNAME);
        System.setProperty("javax.jdo.option.ConnectionPassword", PASSWORD);
    }

    /**
     * Clean up.
     */
    @Override
    public void cleanUp() {

        shutdown();

        if (null != conn) {
            try {
                conn.close();
            } catch (final SQLException e) {
                LOGGER.info("cleanUp, SQLException", e);
            }
        }

        deleteFolder(new File(INPUT_TABLE_FOLDER));

        super.cleanUp();
    }

    /**
     * Shutdown.
     */
    private void shutdown() {

        if (conn != null) {

            try {
                DriverManager.getConnection(DERBY_LOCATION + ";shutdown=true");
            } catch (final SQLException ex) {
                if (((ex.getErrorCode() == 50000) && ("XJ015".equals(ex.getSQLState())))) {
                    LOGGER.debug("Derby shut down  normally");
                } else {
                    LOGGER.debug("Derby did not shut down  normally");
                    LOGGER.debug("Derby did not shut down  normally", ex);
                }
            }

            try {
                conn.close();
            } catch (final SQLException e) {
                LOGGER.debug("shutdown, SQLException", e);
            }
        }
    }
}

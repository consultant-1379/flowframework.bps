package com.ericsson.component.aia.bps.engine.service.spark.mockContexts;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.DEFAULT_INPUT_DATASET;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.EXPECTED_CSV_DATA_SET;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.H2_DRIVER;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Assert;

import com.ericsson.component.aia.bps.core.common.uri.IOURIS;

/**
 * MockJdbcContext is one of the implementation for BaseMockContext and it is useful in creating and validating JDBC related test cases.
 */
public class TestMockJdbcContext extends TestMockBaseContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestMockJdbcContext.class);

    /** The conn. */
    private Connection conn;

    /** The db location. */
    private String DB_LOCATION;

    /**
     * Instantiates a new mock jdbc context.
     *
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public TestMockJdbcContext() throws IOException {

        super(TestMockJdbcContext.class.getSimpleName() + System.currentTimeMillis());

        DB_LOCATION = "jdbc:h2:/" + tmpDir.toAbsolutePath() + SEPARATOR + "H2_JDBC";

        try {
            Class.forName(H2_DRIVER);
            conn = DriverManager.getConnection(DB_LOCATION, "test", "");
            LOGGER.debug("Connection created succesfully");
        } catch (final Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Input configurations for a input source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> inputConfigurations() {

        if (conn != null) {

            Statement stmt = null;

            try {
                stmt = conn.createStatement();
                stmt.executeUpdate("DROP TABLE IF EXISTS sales");
                stmt.executeUpdate("CREATE TABLE sales AS SELECT * FROM CSVREAD('" + DEFAULT_INPUT_DATASET + "');");

                LOGGER.debug("Table created");
            } catch (final Exception e) {
                Assert.fail(e.getMessage());
            }
        }

        final Map<String, String> input = new HashMap<String, String>();
        input.put("uri", IOURIS.JDBC.getUri() + DB_LOCATION);
        input.put("driver", "org.h2.Driver");
        input.put("user", "test");
        input.put("password", "");
        input.put("table.name", "sales");
        return input;
    }

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> outputConfigurations() {
        final Map<String, String> configs = inputConfigurations();
        configs.put("table.name", "sales_output");
        return configs;
    };

    /**
     * Clean up operation for junit test cases.
     */
    @Override
    public void cleanUp() {

        if (null != conn) {
            Statement smt1 = null;
            try {
                smt1 = conn.createStatement();
                // smt1.executeUpdate("DROP TABLE IF EXISTS JDBC");
                smt1.executeUpdate("SHUTDOWN");
                smt1.close();

            } catch (final Exception e) {
                LOGGER.debug("CleanUp: Exception :" + e);
            }

            try {
                conn.close();
            } catch (final SQLException e) {
                LOGGER.debug("CleanUp: Exception :" + e);
            }
            conn = null;
        }
        deleteFolder(tmpDir.toFile());
        super.cleanUp();
    }

    /**
     * Validates expected & actual output data.
     */
    @Override
    public void validate() {
        final String query = "call CSVWRITE ( 'JDBC_OP', 'SELECT * FROM sales_output', null, null,   '' ) ;";
        validateDBOutput(conn, "H2", query, EXPECTED_CSV_DATA_SET);
    }
}

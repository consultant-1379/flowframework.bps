package com.ericsson.component.aia.bps.engine.service.spark.common;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;

import java.util.EnumSet;

import com.ericsson.component.aia.bps.core.common.DataFormat;

/**
 * Various constants used across Spark test module.
 */
public interface TestConstants {

    /** The table header. */
    String TABLE_HEADER = "TRANSACTION_DATE,PRODUCT,PRICE,PAYMENT_TYPE,NAME,CITY,STATE,COUNTRY,ACCOUNT_CREATED,LAST_LOGIN,LATITUDE,LONGITUDE";

    /** The working dir. */
    String WORKING_DIR = System.getProperty("user.dir");

    /** The input data file. */
    String DATA_FILE_NAME = "SalesJan2009";

    /** The expected output dir. */
    String EXPECTED_OUTPUT_DIR = WORKING_DIR + "/src/test/data/expected_output/".replace("/", SEPARATOR);

    /** The expected csv data set. */
    String EXPECTED_CSV_DATA_SET = EXPECTED_OUTPUT_DIR + "expected_output.csv";

    /** The expected file output. */
    String EXPECTED_FILE_OUTPUT = EXPECTED_OUTPUT_DIR + "file_output";

    /** The input data folder. */
    String INPUT_DATA_FOLDER = WORKING_DIR + "/src/test/data/input_data_set/".replace("/", SEPARATOR);

    /** The flow xml. */
    String FLOW_XML = "src" + SEPARATOR + "test" + SEPARATOR + "data" + SEPARATOR + "flow.vm";

    /** The default csv name. */
    String DEFAULT_CSV_NAME = DATA_FILE_NAME + ".csv";

    /** The default input dataset. */
    String DEFAULT_INPUT_DATASET = INPUT_DATA_FOLDER + DEFAULT_CSV_NAME;

    /** The newline. */
    String NEWLINE = "\n";

    /** The hive driver url. */
    String HIVE_URL = "org.apache.hive.jdbc.HiveDriver";

    /** The dfs replication interval. */
    int DFS_REPLICATION_INTERVAL = 1;

    /** The flow. */
    String FLOW = "flow.xml";

    /** The base it folder. */
    String BASE_IT_FOLDER = System.getProperty("java.io.tmpdir") + SEPARATOR + "bps_it";

    /** The root base folder. */
    String ROOT_BASE_FOLDER = BASE_IT_FOLDER + SEPARATOR + "junit_testing_";

    /** The h2 driver. */
    String H2_DRIVER = "org.h2.Driver";

    /** The derby embedded driver. */
    String DERBY_EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

    /** The hive driver. */
    String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    /** The derby client driver. */
    String DERBY_CLIENT_DRIVER = "org.apache.derby.jdbc.ClientDriver";

    /** The true string. */
    String TRUE = "true";

    /** The csv json parquet. */
    EnumSet<DataFormat> CSV_JSON_PARQUET = EnumSet.of(DataFormat.CSV, DataFormat.JSON, DataFormat.PARQUET);
}

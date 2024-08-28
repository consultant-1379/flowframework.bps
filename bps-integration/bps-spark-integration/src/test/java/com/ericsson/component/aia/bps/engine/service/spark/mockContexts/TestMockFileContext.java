package com.ericsson.component.aia.bps.engine.service.spark.mockContexts;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.DATA_FILE_NAME;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.EXPECTED_OUTPUT_DIR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.INPUT_DATA_FOLDER;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.TRUE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.log4j.Logger;
import org.datanucleus.util.StringUtils;
import org.junit.Assert;

import com.ericsson.component.aia.bps.core.common.uri.IOURIS;

/**
 * MockFileContext is one of the implementation for BaseMockContext and it is useful in creating and validating FILE related test cases.
 */
public class TestMockFileContext extends TestMockBaseContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestMockFileContext.class);

    private String outputFolder;

    /**
     * Instantiates a new mock file context.
     */
    public TestMockFileContext() {
        super(TestMockFileContext.class.getSimpleName() + System.currentTimeMillis());
        LOGGER.info("Created Temp Directory--" + tmpDir.toAbsolutePath());
    }

    /**
     * Input configurations for a input source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> inputConfigurations() {

        final String INPUT_FILE = IOURIS.FILE.getUri() + INPUT_DATA_FOLDER + DATA_FILE_NAME + getExtension(inputDataFormat);

        final Map<String, String> input = new HashMap<String, String>();
        input.put("uri", INPUT_FILE);
        input.put("header", TRUE);
        input.put("inferSchema", "false");
        input.put("drop-malformed", TRUE);
        input.put("data.format", getDataFormat(inputDataFormat));
        input.put("skip-comments", TRUE);
        input.put("quoteMode", "ALL");
        input.put("table-name", "sales");
        return input;
    };

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> outputConfigurations() {

        final Map<String, String> output = new HashMap<String, String>();
        outputFolder = tmpDir.toAbsolutePath().toString().replace("\\", "/") + SEPARATOR + "output-folder";
        output.put("uri", IOURIS.FILE.getUri() + outputFolder);

        String dataFormat = getDataFormat(outputDataFormat);

        if (StringUtils.areStringsEqual(dataFormat, "csv")) {
            dataFormat = "com.databricks.spark.csv";
        }

        output.put("data.format", dataFormat);
        return output;
    };

    /**
     * StepConfigurations for a Job as defined in flow xml.
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
     * Validates expected & actual output data.
     */
    @Override
    public void validate() {

        try {
            validateFileOutput(outputFolder, EXPECTED_OUTPUT_DIR + "expected_output" + getExtension(outputDataFormat), outputDataFormat);
        } catch (final Exception e) {
            LOGGER.info("validate operation failed got Exception", e);
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Clean up operation for junit test cases.
     */
    @Override
    public void cleanUp() {

        try {
            FileDeleteStrategy.FORCE.delete(tmpDir.toFile());
        } catch (final IOException e) {
            LOGGER.info("CleanUp, IOException", e);
        }
    }
}

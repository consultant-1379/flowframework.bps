/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2016
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.bps.engine.service.spark.sql_batch_scenarios;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.BASE_IT_FOLDER;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.FLOW;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.FLOW_XML;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.ROOT_BASE_FOLDER;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.ericsson.component.aia.bps.core.common.DataFormat;
import com.ericsson.component.aia.bps.engine.service.BPSPipeLineExecuter;
import com.ericsson.component.aia.bps.engine.service.spark.common.TestUtil;
import com.ericsson.component.aia.bps.engine.service.spark.enums.TestType;
import com.ericsson.component.aia.bps.engine.service.spark.mockContexts.TestMockBaseContext;

/**
 * Integration test suite for BPSPipeLineExecuter Spark Batch.
 */
@Ignore
public abstract class TestBPSPipeLineExecuterBase {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(TestBPSPipeLineExecuterBase.class);

    /** The Constant emptyArray. */
    private static final String[] emptyArray = { "" };

    /** The name. */
    private final String name;

    /** The input type. */
    private final Class<? extends TestMockBaseContext> inputType;

    /** The output type. */
    private final Class<? extends TestMockBaseContext> outputType;

    /** The tmp dir. */
    private Path tmpDir;

    /** The input context. */
    private TestMockBaseContext inputContext;

    /** The output context. */
    private TestMockBaseContext outputContext;

    /** The input data format. */
    private String inputDataFormat;

    /** The output data format. */
    private String outputDataFormat;

    /**
     * Instantiates a new BPS pipe line executer batch test.
     *
     * @param name
     *            the name
     * @param inputType
     *            the input type
     * @param outputType
     *            the output type
     * @param inputDataFormat
     *            the input data format
     * @param outputDataFormat
     *            the output data format
     */
    public TestBPSPipeLineExecuterBase(final String name, final Class<? extends TestMockBaseContext> inputType,
                                       final Class<? extends TestMockBaseContext> outputType, final String inputDataFormat,
                                       final String outputDataFormat) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.name = name;
        this.inputDataFormat = inputDataFormat;
        this.outputDataFormat = outputDataFormat;
    }

    /**
     * Generates Scenarios matrix set.
     *
     * @param fromType
     *            the from type
     * @return the collection
     * @throws Exception
     *             the exception
     */
    public static Collection<Object[]> provideData(final TestType fromType) throws Exception {

        final List<Object[]> scenarios = new ArrayList<>();

        for (final String inputDataFormat : getDataFormatScenarios(fromType.getInputDataFormats())) {

            for (final TestType toType : TestType.values()) {

                for (final String outputDataFormat : getDataFormatScenarios(toType.getOutputDataFormats())) {

                    //                    if (TestType.FILE == fromType && TestType.HDFS == toType && DATA_FORMAT.PARQUET == DATA_FORMAT.valueOf(inputDataFormat)
                    //                            && DATA_FORMAT.CSV == DATA_FORMAT.valueOf(outputDataFormat)) {
                    //
                    //                    } else {
                    //                        continue;
                    //                    }

                    final String inputStr = (StringUtils.isNoneBlank(inputDataFormat)) ? "_" + inputDataFormat : "";
                    final String outputStr = (StringUtils.isNoneBlank(outputDataFormat)) ? "_" + outputDataFormat : "";

                    final Object[] scenario = new Object[5];
                    final Class<? extends TestMockBaseContext> inputContext = fromType.ref;
                    final Class<? extends TestMockBaseContext> outputContext = toType.ref;
                    scenario[0] = fromType.name() + inputStr + " TO " + toType.name() + outputStr;
                    scenario[1] = inputContext;
                    scenario[2] = outputContext;
                    scenario[3] = inputDataFormat;
                    scenario[4] = outputDataFormat;

                    scenarios.add(scenario);
                }
            }
        }

        LOGGER.debug("Finished created integration test scenarios");

        return scenarios;
    }

    /**
     * Gets test scenarios for data formats.
     *
     * @param dataFormats
     *            the data formats
     * @return the scenarios
     */
    public static String[] getDataFormatScenarios(final EnumSet<DataFormat> dataFormats) {

        if (!dataFormats.isEmpty()) {

            final List<String> lst = new ArrayList<String>();
            for (final DataFormat dataFormat : dataFormats) {
                lst.add(dataFormat.name());
            }

            return Arrays.copyOf(lst.toArray(), lst.toArray().length, String[].class);

        }

        return emptyArray;

    }

    /**
     * Creates the base folder.
     *
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void createBaseFolder() throws Exception {

        /*
         * if (new File(BASE_IT_FOLDER).exists()) { FileUtils.forceDelete(new File(BASE_IT_FOLDER)); }
         */
        if (!new File(BASE_IT_FOLDER).exists()) {
            FileUtils.forceMkdir(new File(BASE_IT_FOLDER));
        }
    }

    /**
     * Delete base folder.
     *
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void deleteBaseFolder() throws Exception {

        //        if (new File(BASE_IT_FOLDER).exists()) {
        //            FileUtils.forceDelete(new File(BASE_IT_FOLDER));
        //        }
    }

    /**
     * Creates and initializes folders and different context required for the unit test case.
     *
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception {

        final String dir = ROOT_BASE_FOLDER + this.getClass().getSimpleName();
        final File fileDir = new File(dir);
        fileDir.mkdir();
        tmpDir = fileDir.toPath();
        LOGGER.debug("Created Temp Directory--" + tmpDir.toAbsolutePath());

        System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + tmpDir + SEPARATOR + "junit_metastore_db;create=true");

        System.setProperty("hive.downloaded.resources.dir", tmpDir + SEPARATOR + "hive.resources.dir_" + System.currentTimeMillis());

        if (!inputType.getName().equals(outputType.getName())) {
            inputContext = inputType.newInstance();
            outputContext = outputType.newInstance();
        } else {
            outputContext = inputContext = inputType.newInstance();
        }

        LOGGER.debug("inputDataFormat--" + inputDataFormat);
        LOGGER.debug("outputDataFormat--" + outputDataFormat);

        inputContext.setInputDataFormat(inputDataFormat);
        outputContext.setOutputDataFormat(outputDataFormat);
    }

    /**
     * Executes Test scenario {0} based on Collection<Object[]>.
     */
    @Test
    public void testScenario() {

        LOGGER.debug("Started test scenario");

        /*
         * System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + tmpDir + SEPARATOR + "junit_metastore_db;create=true");
         */
        final Map<String, Map<String, String>> context = new HashMap<>();
        context.put("attributeIPMap", inputContext.inputConfigurations());
        context.put("attributeOPMap", outputContext.outputConfigurations());
        context.put("attributeStepMap", outputContext.stepConfigurations());

        LOGGER.debug("About to execute pipeline");

        executePipeLine(context, tmpDir, name);

        LOGGER.debug("successfully executed pipeline");

        LOGGER.debug("inputType: " + inputType.getName());
        LOGGER.debug("outputType: " + outputType.getName());

        outputContext.validate();

        LOGGER.debug("successfully executed validate");

        LOGGER.debug("Finished test scenario");
    }

    /**
     * Creates the flow xml based on the scenario.
     *
     * @param target_op
     *            the target op
     * @param context
     *            the context
     */
    public static void createFlowXml(final Path target_op, final Map<String, Map<String, String>> context) {

        try {
            TestUtil.createFolder(target_op);
            TestUtil.createXml(FLOW_XML, target_op.toFile().toString(), context, FLOW);

            LOGGER.debug("Create Flow Xml successfully, executing the pipe-line now!!!");
        } catch (final IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Executes test case scenario pipeline based on flow xml.
     *
     * @param context
     *            the context
     * @param target_op
     *            the target op
     * @param scenarioType
     *            the scenario type
     */
    public static void executePipeLine(final Map<String, Map<String, String>> context, final Path target_op, final String scenarioType) {

        LOGGER.debug("Creating Flow xml for the test scenario");

        createFlowXml(target_op, context);

        LOGGER.debug("Created Flow xml for the test scenario");

        LOGGER.debug("Started running pipeline");

        final String flowXML = target_op.toFile().toString() + SEPARATOR + FLOW;

        BPSPipeLineExecuter.main(new String[] { flowXML });

        LOGGER.debug("Started running pipeline");
    }

    /**
     * Clean up operation for junit test cases.
     */
    @After
    public void tearDown() {
        LOGGER.debug("Cleaning up of junit started");

        if (null != tmpDir && tmpDir.toFile().exists()) {
            try {
                FileDeleteStrategy.FORCE.delete(tmpDir.toFile());
            } catch (final IOException e) {
                LOGGER.debug("CleanUp, IOException ", e);
            }
        }

        try {
            inputContext.cleanUp();
            outputContext.cleanUp();
        } catch (final Exception e) {
            LOGGER.debug("CleanUp, Exception", e);
        }

        LOGGER.debug("Cleanup of junit is successful");

    }
}

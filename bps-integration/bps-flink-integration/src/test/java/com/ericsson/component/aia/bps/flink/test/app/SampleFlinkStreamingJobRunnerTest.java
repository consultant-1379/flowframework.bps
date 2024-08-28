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
package com.ericsson.component.aia.bps.flink.test.app;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test class to test SampleFlinkStreamingJobRunner class for Kafka -> Flink Streaming -> Txt
 */
public class SampleFlinkStreamingJobRunnerTest extends AbstractFlinkStreamingJobRunnerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleFlinkStreamingJobRunnerTest.class);

    private static final String EXPECTED_OUTPUT_FILE_PATH = "src/test/resources/ExpectedTextOutput";

    private Properties inputProperties;

    private Properties outputProperties;

    private Properties stepProperties;

    private String flinkOutputDataDir;

    @Before
    public void configureProperties() {
        configureInputProperties();
        configureOutputProperties();
        configureStepProperties();
    }

    @Test
    public void testExtendedKafkaSubscriber() throws InterruptedException, IOException {
        final TestAppExecutor testAppExecutor = new TestAppExecutor();
        testAppExecutor.start();
        LOGGER.info("**************Sleeping for 10 seconds for starting flink test app**************************");
        Thread.sleep(10000);
        produceMessage();
        LOGGER.info("********Sleeping for 5 seconds testExtendedKafkaSubscriber**********");
        Thread.sleep(5000);
        validateOutput();
    }

    private void validateOutput() throws IOException {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final String dateAsString = format.format(Calendar.getInstance().getTime());
        final String actualOutputDataFilePath = flinkOutputDataDir + File.separator + dateAsString;
        assertEquals("The files differ!", FileUtils.readLines(new File(actualOutputDataFilePath), "utf-8").get(0),
                FileUtils.readLines(new File(EXPECTED_OUTPUT_FILE_PATH), "utf-8").get(0));
    }

    private void configureStepProperties() {
        stepProperties = new Properties();
        stepProperties.put("uri", "flink-streaming://EpsFlinkStreamingHandler");
        stepProperties.put("driver-class", "com.ericsson.component.aia.bps.flink.test.app.EpsFlinkStreamingHandler");
    }

    private void configureOutputProperties() {
        flinkOutputDataDir = tempDirectory.toFile().getAbsolutePath() + File.separator + "flink";
        outputProperties = new Properties();
        outputProperties.put("uri", "file:///" + flinkOutputDataDir);
        outputProperties.put("data.format", "txt");
    }

    private void configureInputProperties() {
        inputProperties = new Properties();
        inputProperties.put("uri", "kafka://radio_sesssion_topic");
        inputProperties.put("bootstrap.servers", LOCAL_HOST + ":" + kafkaPort);
        inputProperties.put("zookeeper.connect", LOCAL_HOST + ":" + zookeeperPort);
        inputProperties.put("version", "9");
        inputProperties.put("group.id", "radio_sesssion");
        inputProperties.put("topic", "radio_sesssion_topic");
        inputProperties.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        inputProperties.put("deserialization.schema",
                "com.ericsson.component.aia.common.avro.kafka.flink.decoder.FlinkKafkaGenericRecordDeserializationSchema");
        inputProperties.put("schemaRegistry.address", SCHEMA_DIR);
        inputProperties.put("schemaRegistry.cacheMaximumSize", "50");
    }

    public class TestAppExecutor extends Thread {

        @Override
        public void run() {
            LOGGER.info("**************Starting pipeline KAFKA -> Flink -> TXT **************************");
            final Map<String, Map<String, String>> context = new HashMap<String, Map<String, String>>();
            context.put("attributeIPMap", (Map) inputProperties);
            context.put("attributeOPMap", (Map) outputProperties);
            context.put("attributeStepMap", (Map) stepProperties);
            executePipeLine(context, tempDirectory, "KAFKA-To-TXT");
        }
    }
}

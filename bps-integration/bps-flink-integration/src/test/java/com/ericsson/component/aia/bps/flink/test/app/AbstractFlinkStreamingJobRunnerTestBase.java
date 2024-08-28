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

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.apache.avro.Schema;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.engine.service.BPSPipeLineExecuter;
import com.ericsson.component.aia.common.avro.GenericRecordWrapper;
import com.ericsson.component.aia.common.transport.config.PublisherConfiguration;
import com.ericsson.component.aia.common.transport.service.MessageServiceTypes;
import com.ericsson.component.aia.common.transport.service.Publisher;
import com.ericsson.component.aia.common.transport.service.kafka.KafkaFactory;
import com.ericsson.component.aia.model.registry.client.SchemaRegistryClient;
import com.ericsson.component.aia.model.registry.client.SchemaRegistryItem;

public abstract class AbstractFlinkStreamingJobRunnerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFlinkStreamingJobRunnerTestBase.class);
    /**
     * Zookeeper server.
     */
    private TestingServer zkTestServer;
    /**
     * Kafka server.
     */
    private KafkaServerStartable kafkaServer;
    /**
     * default kafka port which is randomized using findFreePort.
     */
    protected String kafkaPort;
    /**
     * Default kafka host.
     */
    public static final String LOCAL_HOST = "localhost";
    /**
     * Default kafka topic.
     */
    public static final String KAFKA_TOPIC = "radio_sesssion_topic";
    /**
     * Temporary directory to hold intermediate kafka server and zookeeper data.
     */
    protected Path tempDirectory;

    /**
     * default zookeeper port which is randomized using findFreePort.
     */
    protected int zookeeperPort;

    private Properties kafkaProperties;

    protected final String SCHEMA_DIR = "src/test/resources/avro/";

    private ForkableFlinkMiniCluster cluster;

    /**
     * Initialize prerequisite associated with test.
     *
     * @throws Exception
     *             if required things unable to setup.
     */
    @Before
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public void init() throws Exception {
        zookeeperPort = findFreePort();
        kafkaPort = String.valueOf(findFreePort());
        tempDirectory = Files.createTempDirectory("_" + SampleFlinkStreamingJobRunnerTest.class.getCanonicalName());
        zkTestServer = new TestingServer(zookeeperPort, new File(tempDirectory.toFile().getAbsolutePath() + "/tmp_zookeeper"));
        final Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("host.name", "localhost");
        props.put("port", kafkaPort);
        props.put("log.dir", tempDirectory.toFile().getAbsolutePath() + "/tmp_kafka_dir");
        props.put("zookeeper.connect", zkTestServer.getConnectString());
        props.put("replica.socket.timeout.ms", "1500");
        props.put("num.partitions", "1");
        props.put("offsets.topic.num.partitions", "1");
        final KafkaConfig config = new KafkaConfig(props);
        kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();
        configureKafkaProperties();
        startCluster();
    }

    protected void stopCluster() {
        cluster.stop();
    }

    protected void startCluster() {
        final Configuration configuration = new Configuration();
        configuration.setInteger("taskmanager.rpc.port", findFreePort());
        configuration.setInteger("jobmanager.rpc.port", findFreePort());
        cluster = new ForkableFlinkMiniCluster(configuration, false);
        LOGGER.info("********Starting ForkableFlinkMiniCluster**********");
        cluster.start();
    }

    public void configureKafkaProperties() {
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", LOCAL_HOST + ":" + kafkaPort);
        kafkaProperties.put("acks", "1");
        kafkaProperties.put("retries", 0);
        kafkaProperties.put("batch.size", 16384);
        kafkaProperties.put("linger.ms", 1);
        kafkaProperties.put("buffer.memory", 33554432);
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "com.ericsson.component.aia.common.avro.kafka.encoder.KafkaGenericRecordEncoder");
    }

    private KeyedMessage<String, GenericRecordWrapper> generateContextRelMessage(final SchemaRegistryItem SchemaRegistryItemCTX) {
        final GenericRecordWrapper wrapper = getContextRel(SchemaRegistryItemCTX, 0, 30L, 40L, 50L);
        final KeyedMessage<String, GenericRecordWrapper> data = new KeyedMessage<String, GenericRecordWrapper>(KAFKA_TOPIC, String.valueOf(50L),
                wrapper);
        return data;
    }

    private GenericRecordWrapper getContextRel(final SchemaRegistryItem SchemaRegistryItem, final int counts, final Long enbs1apid,
                                               final Long mmes1ap, final Long globalcid) {
        final Long id = SchemaRegistryItem.getId();
        final Schema schema = SchemaRegistryItem.getSchema();
        final ByteBuffer buf = ByteBuffer.allocate(48);
        buf.put("1000".getBytes());

        final GenericRecordWrapper genericRecord = new GenericRecordWrapper(id, schema);
        genericRecord.put("_NE", "t1");
        genericRecord.put("_TIMESTAMP", 1234567L);

        genericRecord.put("TIMESTAMP_HOUR", 11);
        genericRecord.put("TIMESTAMP_MINUTE", 11);
        genericRecord.put("TIMESTAMP_SECOND", 11);
        genericRecord.put("TIMESTAMP_MILLISEC", 11);
        genericRecord.put("SCANNER_ID", 10l);
        genericRecord.put("RBS_MODULE_ID", 400l);
        genericRecord.put("GLOBAL_CELL_ID", globalcid);
        genericRecord.put("ENBS1APID", enbs1apid);
        genericRecord.put("MMES1APID", mmes1ap);
        genericRecord.put("GUMMEI", buf);
        genericRecord.put("RAC_UE_REF", 10l);
        genericRecord.put("TRIGGERING_NODE", 1);
        genericRecord.put("INTERNAL_RELEASE_CAUSE", 345);
        genericRecord.put("S1_RELEASE_CAUSE", 234);
        return genericRecord;
    }

    public static int findFreePort() {
        int port;
        try {
            final ServerSocket socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
            socket.close();
        } catch (final Exception e) {
            port = -1;
        }
        return port;
    }

    public static void executePipeLine(final Map<String, Map<String, String>> context, final Path target_op, final String scenarioType) {

        LOGGER.info("Creating Flow xml for the test scenario");

        createFlowXml(target_op, context);

        LOGGER.info("Created Flow xml for the test scenario");

        LOGGER.info("Started running pipeline");

        final String flowXML = target_op.toFile().toString() + SEPARATOR + "flow.xml";

        BPSPipeLineExecuter.main(new String[] { flowXML });

        LOGGER.info("Started running pipeline");
    }

    private static void createFlowXml(final Path target_op, final Map<String, Map<String, String>> context) {

        try {
            TestUtil.createFolder(target_op);
            TestUtil.createXml("src" + SEPARATOR + "test" + SEPARATOR + "data" + SEPARATOR + "flow.vm", target_op.toFile().toString(), context,
                    "flow.xml");

            LOGGER.info("Initialized Pipe-line successfully, executing pipe-line now!!!");
        } catch (final IOException e) {
            fail(e.getMessage());
        }
    }

    public void produceMessage() {
        LOGGER.info("**************generating message **************************");
        final Properties schemaRegistryClientProperties = new Properties();
        schemaRegistryClientProperties.put("schemaRegistry.address", SCHEMA_DIR);
        schemaRegistryClientProperties.put("schemaRegistry.cacheMaximumSize", "50");
        final SchemaRegistryItem SchemaRegistryItemCTX = SchemaRegistryClient.INSTANCE.configureSchemaRegistryClient(schemaRegistryClientProperties)
                .lookup("celltrace.s.ab11.INTERNAL_PROC_UE_CTXT_RELEASE");
        final KeyedMessage<String, GenericRecordWrapper> data = generateContextRelMessage(SchemaRegistryItemCTX);
        final MessageServiceTypes messageServiceTypes = MessageServiceTypes.KAFKA;
        final PublisherConfiguration config = new PublisherConfiguration(kafkaProperties, messageServiceTypes);
        final Publisher<String, GenericRecordWrapper> producer = KafkaFactory.createKafkaPublisher(config);
        producer.sendMessage(KAFKA_TOPIC, data.key(), data.message());
        LOGGER.info("**************Sent message*******************");
    }

    /**
     * Tear down all services as cleanup.
     *
     * @throws Exception
     *             if unable to tear down successfully.
     */
    @After
    public void tearDown() throws Exception {
        stopCluster();
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
        zkTestServer.stop();
        FileDeleteStrategy.FORCE.deleteQuietly(tempDirectory.toFile());
    }
}

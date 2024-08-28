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
package com.ericsson.component.aia.bps.flink.datasourceservice;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.ericsson.component.aia.common.avro.kafka.flink.decoder.FlinkDeserializationSchema;

/**
 * The <code>BpsFlinkKafkaDataSourceService</code> is responsible for reading data from Kafka (Kafka consumer) and return respective
 * {@link DataStream}.<br>
 *
 * The <code>BpsFlinkKafkaDataSourceService</code> implements <code>BpsDataSourceService&lt;StreamExecutionEnvironment, DataStream&gt;</code> which is
 * specific to StreamExecutionEnvironment & DataStream. <br>
 */
public class BpsFlinkKafkaDataSourceService extends AbstractBpsDataSourceService<StreamExecutionEnvironment, DataStream<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsFlinkKafkaDataSourceService.class);

    @Override
    public String getServiceContextName() {
        LOGGER.trace("Service context name={}", IOURIS.KAFKA.getUri());
        return IOURIS.KAFKA.getUri();
    }

    @Override
    public DataStream<?> getDataStream() {
        final String kafkaBrokers = properties.getProperty("bootstrap.servers");
        // only required for Kafka 0.8, since this class is generic , the property is mandatory
        final String zookeeperConnect = properties.getProperty("zookeeper.connect");
        final String consumerGroupId = properties.getProperty("group.id");
        final String topic = properties.getProperty("topic");
        final String kafkaDeserializationSchemaClassName = properties.getProperty("deserialization.schema");
        final String kafkaVersion = properties.getProperty("version");
        LOGGER.debug(
                "Creating data stream for kafka with version={},brokers={},topic={},consumerGroupId={},deserialization schema={},zookeeperConnect={}",
                kafkaVersion, kafkaBrokers, topic, consumerGroupId, kafkaDeserializationSchemaClassName, zookeeperConnect);
        final FlinkDeserializationSchema<?> KafkaDeserializar = getKafkaDeserializer(kafkaDeserializationSchemaClassName);
        final DataStream<?> stream = context.addSource(getFlinkKafkaConsumer(topic, kafkaVersion, KafkaDeserializar));
        LOGGER.debug("DataStream creation successful for kafka with version={},brokers={},topic={},groupId={},deserializationSchema={},zookeeper={}",
                kafkaVersion, kafkaBrokers, topic, consumerGroupId, kafkaDeserializationSchemaClassName, zookeeperConnect);
        return stream;
    }

    /**
     * @param kafkaDeserializationSchemaClassName
     * @param KafkaDeserializar
     * @return
     */
    private FlinkDeserializationSchema<?> getKafkaDeserializer(final String kafkaDeserializationSchemaClassName) {
        try {
            final FlinkDeserializationSchema<?> KafkaDeserializar = (FlinkDeserializationSchema<?>) BpsFlinkKafkaDataSourceService.class
                    .getClassLoader().loadClass(kafkaDeserializationSchemaClassName).newInstance();
            KafkaDeserializar.setProperties(properties);
            return KafkaDeserializar;
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException exp) {
            throw new IllegalArgumentException(String.format("Unable to create class with name=%s", kafkaDeserializationSchemaClassName), exp);
        }
    }

    private FlinkKafkaConsumerBase<?> getFlinkKafkaConsumer(final String topic, final String kafkaVersion,
            final FlinkDeserializationSchema<?> KafkaDeserializar) {
        final int kafkaVer = Integer.parseInt(kafkaVersion);
        if (8 == kafkaVer) {
            return new FlinkKafkaConsumer08<>(topic, KafkaDeserializar, properties);
        } else if (9 == kafkaVer) {
            return new FlinkKafkaConsumer09<>(topic, KafkaDeserializar, properties);
        } else {
            throw new IllegalArgumentException("Only kafka 8 and 9 versions are supported");
        }

    }
}

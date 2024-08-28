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
package com.ericsson.component.aia.bps.spark.datasinkservice;

import java.io.Serializable;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.URIDefinition;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.common.avro.utilities.GenericRecordConversionException;
import com.ericsson.component.aia.common.avro.utilities.SparkRddToAvro;
import com.ericsson.component.aia.common.transport.kafka.utilities.KafkaAvroWriter;

/**
 * The <code>BpsSparkFileDataSink</code> class is responsible for writing {@link DataFrame } to a Kafka topic.<br>
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
public class BpsSparkKafkaDataSink<C> extends BpsAbstractDataSink<C, DataFrame> implements Serializable {

    private static final long serialVersionUID = -4916428491473581453L;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkKafkaDataSink.class);

    KafkaAvroWriter writer;
    String eventType;

    /**
     * Configured instance of {@link BpsSparkKafkaDataSink} for the specified sinkContextName.<br>
     * sinkContextName is the name of the output sink which was configured in flow.xml through tag &lt;output name="sinkContextName"&gt; ....
     * &lt;/output&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param properties
     *            the configuration associated with underlying output sink.
     * @param sinkContextName
     *            Unique name associated with each of the output sink.
     */
    @Override
    @SuppressWarnings("PMD.AvoidThrowingRawExceptionTypes")
    public void configureDataSink(final C context, final Properties properties, final String sinkContextName) {
        super.configureDataSink(context, properties, sinkContextName);
        LOGGER.trace(String.format("Initiating configureDataSink for %s. ", getDataSinkContextName()));
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties.getProperty(Constants.URI));
        final Properties params = decode.getParams();
        eventType = properties.getProperty("eventType");
        final String format = params.getProperty("format");

        if (!"avro".equalsIgnoreCase(format)) {
            throw new RuntimeException("Currenlty Kafka support avro format only");
        }
        writer = KafkaAvroWriter.Builder.create().withBrokers(properties.getProperty("bootstrap.servers"))
                .withKeySerializer("org.apache.kafka.common.serialization.StringSerializer")
                .withValueSerializer("com.ericsson.component.aia.common.avro.kafka.encoder.KafkaGenericRecordEncoder").withTopic(decode.getContext())
                .build();

        LOGGER.info(String.format("Configuring %s for the output  %s for Kafka topic %s", this.getClass().getName(), sinkContextName,
                decode.getContext()));
        LOGGER.trace(String.format("Finished configureDataSink method for the sink context name %s ", sinkContextName));
    }

    @Override
    public void cleanUp() {
        LOGGER.trace(String.format("Cleaning resources allocated for %s ", getDataSinkContextName()));
        strategy = null;
        writer.close();
        LOGGER.trace(String.format("Cleaned resources allocated for %s ", getDataSinkContextName()));
    }

    @Override
    public String getServiceContextName() {
        return IOURIS.KAFKA.getUri();
    }

    @Override
    public void write(final DataFrame dataStream) {
        LOGGER.info("BpsSparkKafkaDataSink [Context=" + getDataSinkContextName() + "] got Dataframe with [Rows=" + dataStream.count() + "]");
        final JavaRDD<GenericRecord> transformRddToAvroRdd;
        try {
            transformRddToAvroRdd = SparkRddToAvro.transformRddToAvroRdd(dataStream.javaRDD(), eventType);
            transformRddToAvroRdd.foreach(new VoidFunction<GenericRecord>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void call(final GenericRecord t) {
                    writer.write(t);
                }
            });
        } catch (final GenericRecordConversionException e) {
            LOGGER.error("BpsSparkKafkaDataSink [Context=" + getDataSinkContextName() + "] got error while processing dataframe with [Rows="
                    + dataStream.count() + "]", e);
        } finally {
            writer.flush();
        }
        LOGGER.info("BpsSparkKafkaDataSink [Context=" + getDataSinkContextName() + "] write operation completed successfuly.");
    }
}

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
package com.ericsson.component.aia.bps.spark.jobrunner.common;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.bps.core.utils.PropertiesReader;
import com.ericsson.component.aia.bps.spark.configuration.KafkaStreamConfiguration;
import com.ericsson.component.aia.common.avro.kafka.decoder.KafkaGenericRecordDecoder;

import kafka.serializer.StringDecoder;

/**
 * This class provides useful context initializations related to the spark related operations.
 */
public class SparkAppHeader implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -7684414561487437994L;

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(SparkAppHeader.class);

    /** The jssc. */
    private static transient JavaStreamingContext jssc;

    /** The spark conf. */
    private static SparkConf sparkConf;

    private SparkAppHeader() {

    }

    /**
     * A method to create kafka stream.
     *
     * @param conf
     *            is an instance of {@link KafkaStreamConfiguration} contains the configuration for the kafka connections
     * @return instance of {@link JavaPairInputDStream}
     */
    public static JavaPairInputDStream<String, GenericRecord> createConnection(final KafkaStreamConfiguration conf) {
        LOG.trace("Entering the createConnection method"); // Create direct
        // kafka
        // stream with
        // brokers
        // and topics
        final JavaPairInputDStream<String, GenericRecord> messages = KafkaUtils.createDirectStream(jssc, String.class, GenericRecord.class,
                StringDecoder.class, KafkaGenericRecordDecoder.class, conf.getKafkaParams(), conf.getTopicsSet());
        LOG.trace("returning the createConnection method");
        return messages;
    }

    /**
     * This method creates basic context for the spark.
     *
     * @param bpsInput
     *            is the mapped inputs from flow xml file
     * @param bpsOut
     *            is the mapped output from the flow xml file
     * @param properties
     *            the properties
     * @return instant of {@link JavaSparkContext}
     */
    @SuppressWarnings("unchecked")
    public static JavaSparkContext initSparkContext(final BpsDataSourceAdapters bpsInput, final BpsDataSinkAdapters bpsOut,
                                                    final Properties properties) {
        LOG.trace("Initializing spark context()-->");
        final CompositeConfiguration sparkJobConfig = PropertiesReader.getProperties("sparkDefault", properties);

        sparkConf = new SparkConf().setAppName(sparkJobConfig.getString("step.name")).setMaster(sparkJobConfig.getString("master.url"));

        final Iterator<String> keys = sparkJobConfig.getKeys();

        while (keys.hasNext()) {
            final String key = keys.next();
            sparkConf.set(key, sparkJobConfig.getString(key));
        }

        if (null != properties.getProperty("master.url")) {
            LOG.info("Detected a spark master url , So initializing non local mode");
        }

        // Create the context with a 1 second batch size
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        LOG.info("Initialized the Java Spark Context.");
        LOG.trace("Initializing spark context()<--");
        return ctx;
    }

    /**
     * A method to initialize the spark streaming context using the components provided.
     *
     * @param bpsInput
     *            is the mapped inputs from flow xml file
     * @param bpsOut
     *            is the mapped output from the flow xml file
     * @param sparkJobConfig
     *            the spark job config
     * @return instant of {@link JavaStreamingContext}
     */
    public static JavaStreamingContext init(final BpsDataSourceAdapters bpsInput, final BpsDataSinkAdapters bpsOut, final Properties sparkJobConfig) {
        LOG.trace("Entering the ProxyHandler method");
        sparkConf = new SparkConf().setAppName(sparkJobConfig.getProperty("app-name")).setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.externalBlockStore.url", sparkJobConfig.getProperty("spark.externalBlockStore.url"));
        // Create the context with a 1 second batch size
        jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
        jssc.checkpoint(sparkJobConfig.getProperty("streaming.checkpoint", "/tmp/"));
        LOG.info("Initialized the Java Spark Streaming Context.");
        LOG.trace("Initializing Spark Streaming context()-->");
        return jssc;
    }
}

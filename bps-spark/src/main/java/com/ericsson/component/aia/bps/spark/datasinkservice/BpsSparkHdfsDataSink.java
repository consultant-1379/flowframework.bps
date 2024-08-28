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

import java.util.Properties;

import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.URIDefinition;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.partition.BpsPartition;
import com.ericsson.component.aia.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.bps.spark.configuration.partition.SparkDefaultPartition;

/**
 * The <code>BpsSparkHdfsDataSink</code> class is responsible for writing {@link DataFrame } to a HDFS.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
public class BpsSparkHdfsDataSink<C> extends BpsAbstractDataSink<C, DataFrame> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkHdfsDataSink.class);

    /** The Partition strategy used by the Hdfs Data Sink. */
    protected BpsPartition<DataFrame> strategy;

    /**
     * Configured instance of {@link BpsSparkHdfsDataSink} for the specified sinkContextName.<br>
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
    @SuppressWarnings("CPD-START")
    public void configureDataSink(final C context, final Properties properties, final String sinkContextName) {
        super.configureDataSink(context, properties, sinkContextName);
        final URIDefinition<IOURIS> decode = IOURIS.decode(properties.getProperty(Constants.URI));
        super.writingContext = decode.getProtocol().getUri() + decode.getContext();
        LOGGER.trace(String.format("Initiating configureDataSink for %s. ", getDataSinkContextName()));
        strategy = new SparkDefaultPartition();
        LOGGER.info(String.format("Configuring %s for the output  %s with partition strategy %s", this.getClass().getName(), sinkContextName,
                strategy.getClass().getName()));
        strategy.init(properties);
        LOGGER.trace(String.format("Finished configureDataSink method for the sink context name %s ", sinkContextName));
    }

    /**
     * Writes DataFrame to HDFS location based on Partition strategy
     */
    @Override
    @SuppressWarnings("CPD-END")
    public void write(final DataFrame dataStream) {
        LOGGER.trace(String.format("Initiating Write for %s. ", getDataSinkContextName()));
        final String path = getWritingContext();
        strategy.write(dataStream, path);
        LOGGER.trace(String.format("Finished Write for %s. ", getDataSinkContextName()));
    }

    @Override
    public void cleanUp() {
        LOGGER.trace(String.format("Cleaning resources allocated for %s ", getDataSinkContextName()));
        strategy = null;
        LOGGER.trace(String.format("Cleaned resources allocated for %s ", getDataSinkContextName()));
    }

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.HDFS.getUri();
    }
}

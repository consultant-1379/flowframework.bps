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
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.URIDefinition;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.bps.spark.configuration.partition.SparkDefaultPartition;

/**
 * The <code>BpsSparkTachyonDataSink</code> class is responsible for writing {@link DataFrame } to a Tachyon.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
public class BpsSparkTachyonDataSink<C> extends BpsAbstractDataSink<C, DataFrame> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkTachyonDataSink.class);

    /** The t location. */
    private String tLocation;

    /** The is partition enabled. */
    private boolean isPartitionEnabled;

    /** The p columns. */
    private String[] pColumns;

    /** The d format. */
    private String dFormat;

    /** The is text format. */
    private boolean isTextFormat;

    /** The write mode. */
    private Object writeMode;

    /** The is rdd format. */
    private boolean isRddFormat;

    /**
     * Configured instance of {@link BpsSparkTachyonDataSink} for the specified sinkContextName.<br>
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
    public void configureDataSink(final C context, final Properties properties, final String sinkContextName) {
        super.configureDataSink(context, properties, sinkContextName);
        LOGGER.trace(String.format("Initiating configureDataSink for %s. ", getDataSinkContextName()));
        final String uri = properties.getProperty(Constants.URI);
        final URIDefinition<IOURIS> decode = IOURIS.decode(uri);
        tLocation = properties.getProperty("master-url") + "/" + decode.getContext();
        final String partitionColumes = properties.getProperty("partition.columns");
        isPartitionEnabled = isPartitionEnabled(partitionColumes);
        if (isPartitionEnabled) {
            pColumns = partitionColumes.split(",");
        }
        final String property = properties.getProperty("data.format");
        dFormat = (property == null) ? "parquet" : property;
        isTextFormat = ("text").equalsIgnoreCase(dFormat);
        isRddFormat = ("rdd").equalsIgnoreCase(dFormat);
        final String saveMode = properties.getProperty("data.save.mode", "Append");
        writeMode = (saveMode == null) ? SaveMode.Append : getMode(property);
        strategy = new SparkDefaultPartition();
        strategy.init(properties);
        LOGGER.trace(String.format("Finished configureDataSink method for the sink context name %s ", sinkContextName));
    }

    /**
     * @param partitionColumes
     * @return
     */
    private boolean isPartitionEnabled(final String partitionColumes) {
        return (partitionColumes == null || partitionColumes.trim().length() < 1) ? false : true;
    }

    /**
     * Writes DataFrame to Tachyon/Alluxio
     */
    @Override
    public void write(final DataFrame frame) {
        LOGGER.trace(String.format("Initiating Write for %s. ", getDataSinkContextName()));
        if (isTextFormat) {
            LOGGER.trace(String.format("Writing for %s as TextFormat ", getDataSinkContextName()));
            frame.rdd().saveAsTextFile(tLocation);
            return;
        }

        if (!isPartitionEnabled) {
            LOGGER.trace(String.format("Writing for %s with no partition strategy", getDataSinkContextName()));
            frame.write().mode(SaveMode.Append).format(dFormat).save(tLocation);
            return;
        }

        if (isRddFormat) {
            LOGGER.trace(String.format("Writing for %s as data received in RDD", getDataSinkContextName()));
            frame.rdd().saveAsObjectFile(tLocation);
            return;
        }

        LOGGER.trace(String.format("Writing for %s as with SaveMode %s format %s  partation by {%s} at location %s", getDataSinkContextName(),
                SaveMode.Append, dFormat, pColumns, tLocation));
        frame.write().mode(SaveMode.Append).format(dFormat).partitionBy(pColumns).save(tLocation);
        LOGGER.trace(String.format("Finished Write for %s. ", getDataSinkContextName()));
    }

    /**
     * Gets the write mode.
     *
     * @param mode
     *            the mode
     * @return the mode
     */
    private SaveMode getMode(final String mode) {
        LOGGER.trace(String.format("Initiating getMode method for %s with mode value %s ", getDataSinkContextName(), mode));
        if (("Overwrite").equalsIgnoreCase(mode)) {
            return SaveMode.Overwrite;
        }
        if (("Append").equalsIgnoreCase(mode)) {
            return SaveMode.Append;
        }
        if (("ErrorIfExists").equalsIgnoreCase(mode)) {
            return SaveMode.ErrorIfExists;
        }
        if (("Ignore").equalsIgnoreCase(mode)) {
            return SaveMode.Ignore;
        }
        LOGGER.trace(String.format("Finished getMode for %s. ", getDataSinkContextName()));
        return SaveMode.Append;
    }

    /**
     * Gets the write mode.
     *
     * @return the write mode
     */
    public Object getWriteMode() {
        return writeMode;
    }

    @Override
    public void cleanUp() {
        LOGGER.trace(String.format("Cleaning resources allocated for %s ", getDataSinkContextName()));
        strategy = null;
        writeMode = null;
        LOGGER.trace(String.format("Cleaned resources allocated for %s ", getDataSinkContextName()));
    }

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.ALLUXIO.getUri();
    }
}

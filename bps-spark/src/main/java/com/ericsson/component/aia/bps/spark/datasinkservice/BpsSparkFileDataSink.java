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

import java.io.File;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.partition.BpsPartition;
import com.ericsson.component.aia.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.bps.spark.configuration.partition.SparkDefaultPartition;

/**
 * The <code>BpsSparkFileDataSink</code> class is responsible for writing {@link DataFrame } to a file.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
public class BpsSparkFileDataSink<C> extends BpsAbstractDataSink<C, DataFrame> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkFileDataSink.class);

    /** The Partition strategy used by the FileDataSink. */
    private BpsPartition<DataFrame> strategy;

    /** The method. */
    private TimeUnit method;

    /** The data format. */
    private String dataFormat;

    /**
     * Configured instance of {@link BpsSparkFileDataSink} for the specified sinkContextName.<br>
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
        strategy = new SparkDefaultPartition();
        LOGGER.info(String.format("Configuring %s for the output  %s with partition strategy %s", this.getClass().getName(), sinkContextName,
                strategy.getClass().getName()));
        strategy.init(properties);
        LOGGER.trace(String.format("Finished configureDataSink method for the sink context name %s ", sinkContextName));
    }

    /**
     * The method will try to delete all the files and folder recursively starting from the folder/file name provided to the method. The
     * {@link BpsSparkFileDataSink#write(DataFrame)} methods uses {@link BpsSparkFileDataSink#delete(File)} methods in order to delete the files and
     * folder before writing new data to the provided path.
     *
     * @param parentFolder
     *            Try to delete all the files belongs to parent folder.
     */
    void delete(final File parentFolder) {

        LOGGER.trace(String.format("Try to delete %s ", parentFolder.getAbsolutePath()));
        if (parentFolder.isDirectory()) {
            for (final File childFile : parentFolder.listFiles()) {
                delete(childFile);
            }
        }
        if (!parentFolder.delete()) {
            throw new IllegalStateException(String.format("Failed to deleted %s ", parentFolder.getAbsolutePath()));
        }
        LOGGER.trace(String.format("Delete successfully %s ", parentFolder.getAbsolutePath()));
    }

    /**
     * Writes DataFrame to file location based on Partition strategy
     */
    @Override
    public void write(final DataFrame dataSet) {
        LOGGER.trace(String.format("Initiating Write for %s. ", getDataSinkContextName()));
        if (IOURIS.getUriType(getProperties()) == IOURIS.FILE.getUri()) {
            final File file = new File(URI.create(getProperties().getProperty(Constants.URI)).getPath());
            if (file.exists()) {
                delete(file);
            }
        }
        strategy.write(dataSet, getWritingContext());
        LOGGER.trace(String.format("Finished Write for %s. ", getDataSinkContextName()));
    }

    /**
     * Gets the method.
     *
     * @return the method
     */
    public TimeUnit getMethod() {
        return method;
    }

    /**
     * Sets the method of unit.
     *
     * @param method
     *            the new method
     */
    public void setMethod(final TimeUnit method) {
        this.method = method;
    }

    /**
     * Gets the data format.
     *
     * @return the dataFormat
     */
    public String getDataFormat() {
        return dataFormat;
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
        return IOURIS.FILE.getUri();
    }
}

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
package com.ericsson.component.aia.bps.flink.datasinkservice;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.streams.BpsAbstractDataSink;
import com.google.common.base.Preconditions;

/**
 * The <code>BpsFlinkFileDataSink</code> class is responsible for writing {@link DataFrame } to a file.<br>
 *
 * @param <C>
 *            the generic type representing the context like {@link StreamExecutionEnvironment} etc.
 */
public class BpsFlinkFileDataSink<C> extends BpsAbstractDataSink<C, DataStream<?>> {

    private static final String SEPARATOR = File.separator;

    private static final String TEXT_DATA_FORMAT = "txt";

    private static final String CSV_DATA_FORMAT = "csv";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsFlinkFileDataSink.class);

    private final Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    /** The method. */
    private TimeUnit method;

    /** The data format. */
    private String dataFormat;

    /**
     * Configured instance of {@link BpsFlinkFileDataSink} for the specified sinkContextName.<br>
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
        LOGGER.info(String.format("Configuring %s for the output ", this.getClass().getName(), sinkContextName));
        LOGGER.trace(String.format("Finished configureDataSink method for the sink context name %s ", sinkContextName));
    }

    /**
     * The method will try to delete all the files and folder recursively starting from the folder/file name provided to the method. The
     * {@link BpsFlinkFileDataSink#write(DataFrame)} methods uses {@link BpsFlinkFileDataSink#delete(File)} methods in order to delete the files and
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
    public void write(final DataStream<?> dataSet) {
        LOGGER.trace("Initiating Write for {}", getDataSinkContextName());
        if (dataSet == null) {
            LOGGER.info("Record null returning...");
            return;
        }
        final String dataFormat = getProperties().getProperty("data.format");
        Preconditions.checkArgument(dataFormat != null, "Invalid data.format");
        LOGGER.info("Record not null writing now...");
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final String dateAsString = format.format(cal.getTime()) + SEPARATOR;
        if (TEXT_DATA_FORMAT.equalsIgnoreCase(dataFormat)) {
            dataSet.writeAsText(getWritingContext() + SEPARATOR + dateAsString + SEPARATOR, WriteMode.OVERWRITE).setParallelism(1);
        } else if (CSV_DATA_FORMAT.equalsIgnoreCase(dataFormat)) {
            dataSet.writeAsCsv(getWritingContext() + SEPARATOR + dateAsString + SEPARATOR, WriteMode.OVERWRITE);
        }
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
        LOGGER.trace("Cleaning resources allocated for {} ", getDataSinkContextName());
        strategy = null;
        LOGGER.trace("Cleaned resources allocated for {} ", getDataSinkContextName());
    }

    @Override
    public String getServiceContextName() {
        return IOURIS.FILE.getUri();
    }
}

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
package com.ericsson.component.aia.bps.spark.configuration.partition;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.service.partition.BpsPartition;
import com.ericsson.component.aia.bps.spark.utils.SparkUtil;
import com.google.common.base.Preconditions;

/**
 * The Class UserDefinedPartition.
 */
public class SparkDefaultPartition implements BpsPartition<DataFrame> {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(SparkDefaultPartition.class);

    /** The cal. */
    private final Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    /** The partion columes. */
    private String partionColumes;

    /** The properties. */
    private Properties properties;

    /** The is partition enabled. */
    private boolean isPartitionEnabled;

    /** The p columns. */
    private String[] pColumns;

    /** The d format. */
    private String dFormat;

    /** The save mode. */
    private String saveMode;

    /** The write mode. */
    private SaveMode writeMode;

    /** The is text format. */
    private boolean isTextFormat;

    /**
     * Gets the date part.
     *
     * @param root
     *            the root
     * @param unit
     *            the unit
     * @return the date part
     */
    @SuppressWarnings("unused")
    private String getDatePart(final String root, final TimeUnit unit) {
        LOG.trace("Entering the getDatePart method");
        switch (unit) {
            case DAYS:
                final String part = cal.get(Calendar.YEAR) + SEPARATOR + cal.get(Calendar.MONTH) + SEPARATOR + cal.get(Calendar.DATE);
                return root + SEPARATOR + part;
            case HOURS:
                final String hrpart = cal.get(Calendar.YEAR) + SEPARATOR + cal.get(Calendar.MONTH) + SEPARATOR + cal.get(Calendar.DATE) + SEPARATOR
                        + cal.get(Calendar.HOUR);
                return root + SEPARATOR + hrpart;
            case MINUTES:
                final String minpart = cal.get(Calendar.YEAR) + SEPARATOR + cal.get(Calendar.MONTH) + SEPARATOR + cal.get(Calendar.DATE) + SEPARATOR
                        + cal.get(Calendar.HOUR) + SEPARATOR + cal.get(Calendar.MINUTE) + SEPARATOR;
                return root + SEPARATOR + minpart;

            default:
                throw new UnsupportedOperationException("Too smal partition requested.");
        }
    }

    /**
     * Gets the unit.
     *
     * @param method
     *            the method
     * @return the unit
     */
    public static TimeUnit getUnit(final String method) {
        LOG.trace("Entering the getUnit method");
        Preconditions.checkArgument(method != null && method.trim().length() > 0, "Time unit method cannot be null");
        if ("days".equalsIgnoreCase(method)) {
            return TimeUnit.DAYS;
        }
        if ("hh".equalsIgnoreCase(method)) {
            return TimeUnit.HOURS;
        }
        if ("mm".equalsIgnoreCase(method)) {
            return TimeUnit.MINUTES;
        }
        LOG.trace("Existing the getUnit method");
        throw new UnsupportedOperationException("unknown time format requested for partitions");
    }

    /**
     * Gets the user defined partitions.
     *
     * @param props
     *            the props
     * @return the user defined partitions
     */
    public String[] getUserDefinedPartitions(final Properties props) {
        LOG.trace("Entering the getUserDefinedPartitions method");
        final String property = props.getProperty("partition.columns");
        LOG.trace("Existing the getUserDefinedPartitions method");
        return property.split(",");
    }

    /**
     * Initializes UDF partition
     *
     */
    @SuppressWarnings("PMD.NPathComplexity")
    @Override
    public void init(final Properties props) {

        LOG.trace("Entering the init method");

        this.properties = props;
        final String partitionColumes = props.getProperty("partition.columns");
        isPartitionEnabled = ((partitionColumes == null || partitionColumes.trim().length() < 1) ? false : true);

        if (isPartitionEnabled) {
            pColumns = partitionColumes.split(",");
        }

        final String property = props.getProperty("data.format");
        dFormat = (null == property) ? "parquet" : property;
        isTextFormat = ("text").equalsIgnoreCase(dFormat);
        final String saveMode = props.getProperty("data.save.mode", "Append");
        writeMode = (saveMode == null) ? SaveMode.Append : getMode(property);

        LOG.trace("Existing the init method");
    }

    /**
     * Gets the persisting mode.
     *
     * @param mode
     *            the mode
     * @return the mode
     */
    private SaveMode getMode(final String mode) {

        LOG.trace("Entering the getMode method");

        if (mode == null) {
            return SaveMode.Append;
        } else if (("Overwrite").equalsIgnoreCase(mode)) {
            return SaveMode.Overwrite;
        } else if (("Append").equalsIgnoreCase(mode)) {
            return SaveMode.Append;
        } else if (("ErrorIfExists").equalsIgnoreCase(mode)) {
            return SaveMode.ErrorIfExists;
        } else if (("Ignore").equalsIgnoreCase(mode)) {
            return SaveMode.Ignore;
        }

        LOG.trace("Existing the getMode method");

        return SaveMode.Append;
    }

    /**
     * Writes DataFrame to a path based on Partition strategy
     */
    @Override
    public void write(final DataFrame frame, final String path) {

        LOG.trace("Entering the write method");

        if (isTextFormat) {
            write(frame.javaRDD().coalesce(1), path);
            return;
        }

        final Map<String, String> options = SparkUtil.getOptions(properties);

        frame.write().format(dFormat).mode(writeMode).options(options).partitionBy(pColumns).save(path);

        LOG.trace("Existing the write method");
    }

    /**
     * Write.
     *
     * @param <T>
     *            the generic type
     * @param rdd
     *            the rdd
     * @param path
     *            the path
     */
    public <T> void write(final JavaRDD<T> rdd, final String path) {
        LOG.trace("Entering the write method");
        rdd.saveAsTextFile(path);
        LOG.trace("Existing the write method");
    }

    /**
     * Gets the partion columes.
     *
     * @return the partionColumes
     */
    protected String getPartionColumes() {
        return partionColumes;
    }

    /**
     * Sets the partion columes.
     *
     * @param partionColumes
     *            the partionColumes to set
     */
    protected void setPartionColumes(final String partionColumes) {
        this.partionColumes = partionColumes;
    }

    /**
     * Gets the properties.
     *
     * @return the properties
     */
    protected Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties.
     *
     * @param properties
     *            the properties to set
     */
    protected void setProperties(final Properties properties) {
        this.properties = properties;
    }

    /**
     * Checks if is partition enabled.
     *
     * @return the isPartitionEnabled
     */
    protected boolean isPartitionEnabled() {
        return isPartitionEnabled;
    }

    /**
     * Sets the partition enabled.
     *
     * @param isPartitionEnabled
     *            the isPartitionEnabled to set
     */
    protected void setPartitionEnabled(final boolean isPartitionEnabled) {
        this.isPartitionEnabled = isPartitionEnabled;
    }

    /**
     * Gets the p columns.
     *
     * @return the pColumns
     */
    protected String[] getpColumns() {
        return pColumns;
    }

    /**
     * Sets the p columns.
     *
     * @param pColumns
     *            the pColumns to set
     */
    protected void setpColumns(final String[] pColumns) {
        this.pColumns = pColumns;
    }

    /**
     * Gets the d format.
     *
     * @return the dFormat
     */
    protected String getdFormat() {
        return dFormat;
    }

    /**
     * Sets the d format.
     *
     * @param dFormat
     *            the dFormat to set
     */
    protected void setdFormat(final String dFormat) {
        this.dFormat = dFormat;
    }

    /**
     * Gets the save mode.
     *
     * @return the saveMode
     */
    protected String getSaveMode() {
        return saveMode;
    }

    /**
     * Sets the save mode.
     *
     * @param saveMode
     *            the saveMode to set
     */
    protected void setSaveMode(final String saveMode) {
        this.saveMode = saveMode;
    }

    /**
     * Gets the write mode.
     *
     * @return the writeMode
     */
    protected SaveMode getWriteMode() {
        return writeMode;
    }

    /**
     * Sets the write mode.
     *
     * @param writeMode
     *            the writeMode to set
     */
    protected void setWriteMode(final SaveMode writeMode) {
        this.writeMode = writeMode;
    }

    /**
     * Gets the partition.
     *
     * @return the partition
     */
    public String getPartition() {
        return partionColumes;
    }
}

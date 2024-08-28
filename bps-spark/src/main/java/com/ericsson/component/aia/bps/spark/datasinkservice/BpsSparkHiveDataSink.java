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
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.bps.spark.configuration.partition.SparkDefaultPartition;
import com.ericsson.component.aia.bps.spark.utils.TransformerUtils;

/**
 * The <code>BpsSparkHiveDataSink</code> class is responsible for writing {@link DataFrame } to a Hive.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
public class BpsSparkHiveDataSink<C> extends BpsAbstractDataSink<C, DataFrame> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkHiveDataSink.class);

    /** The format. */
    private String format;

    /** The is partition enabled. */
    private boolean isPartitionEnabled;

    /** The p columns. */
    private String[] pColumns;

    /**
     * Configured instance of {@link BpsSparkHiveDataSink} for the specified sinkContextName.<br>
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
        final String partitionColumes = properties.getProperty(Constants.PARTITION_COLUMNS);
        format = properties.getProperty(Constants.DATA_FORMAT);
        isPartitionEnabled = ((partitionColumes == null || partitionColumes.trim().length() < 1) ? false : true);

        if (isPartitionEnabled) {
            pColumns = partitionColumes.split(",");
        }
        strategy = new SparkDefaultPartition();
        LOGGER.info(String.format("Configuring %s for the output  %s with partition strategy %s", this.getClass().getName(), sinkContextName,
                strategy.getClass().getName()));
        strategy.init(properties);
        LOGGER.trace(String.format("Finished configureDataSink method for the sink context name %s ", sinkContextName));
    }

    /**
     * Writes DataFrame to Hive table based on Partition strategy
     */
    @Override
    public void write(final DataFrame frame) {
        LOGGER.trace(String.format("Initiating Write for %s. ", getDataSinkContextName()));
        final long time = System.currentTimeMillis();
        final String tableName = "tempTable" + time;
        frame.registerTempTable(tableName);
        frame.dropDuplicates();
        // need a fix later
        final String ddl = TransformerUtils.buildDDL(frame.schema(), null, getWritingContext(),
                (format == null || format.trim().length() < 0) ? "PARQUET" : format);
        LOGGER.info("Loading data into hive table {}", getWritingContext());
        final DataFrame sql = ((HiveContext) getContext()).sql(ddl);
        if (sql.count() == 0) {
            frame.write().mode(SaveMode.Append).format((format == null || format.trim().length() < 0) ? "PARQUET" : format)
                    .saveAsTable(getWritingContext());
        }
        frame.sqlContext().dropTempTable(tableName);
        LOGGER.trace(String.format("Finished Write for %s. ", getDataSinkContextName()));
    }

    /**
     * Gets the p columns.
     *
     * @return the p columns
     */
    public String[] getpColumns() {
        return pColumns;
    }

    /**
     * Sets the p columns.
     *
     * @param pColumns
     *            the new p columns
     */
    public void setpColumns(final String[] pColumns) {
        this.pColumns = pColumns;
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
        return IOURIS.HIVE.getUri();
    }
}

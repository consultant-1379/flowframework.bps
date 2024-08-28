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
package com.ericsson.component.aia.bps.spark.jobrunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.URIDefinition;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceConfiguration;
import com.ericsson.component.aia.bps.core.service.streams.BpsBaseStream;
import com.ericsson.component.aia.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.bps.core.utils.PropertiesReader;
import com.ericsson.component.aia.bps.spark.datasinkservice.BpsSparkFileDataSink;
import com.ericsson.component.aia.bps.spark.jobrunner.common.SparkAppFooter;
import com.ericsson.component.aia.bps.spark.jobrunner.common.SparkAppHeader;
import com.google.common.base.Preconditions;

/**
 * LocalSparkSQLHandler class is a one of the implementation for Step interface. This handler is used when user wants to run File to file use-case
 * batch scenario in Spark local mode.
 *
 * This class is used in local testing mode.
 */
public class LocalSparkSQLJobRunner implements BpsJobRunner {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 655833713003724650L;

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(LocalSparkSQLJobRunner.class);

    /** The properties. */
    private Properties properties;

    /** The streams. */
    private transient BpsInputStreams streams;

    /** The jssc. */
    private JavaSparkContext jssc;

    /** The context. */
    private SQLContext context;

    /** The writers. */
    private final transient List<BpsSparkFileDataSink<SQLContext>> writers = new ArrayList<>();

    @Override
    /**
     * Initializes step handler parameters.
     *
     * @param inputAdapters
     *            the input adapters
     * @param outputAdapters
     *            the output adapters
     * @param properties
     *            the properties
     */
    public void initialize(final BpsDataSourceAdapters inputAdapters, final BpsDataSinkAdapters outputAdapters, final Properties properties) {

        LOG.trace("Entering the initialize method");

        this.properties = properties;

        if (jssc == null) {
            jssc = SparkAppHeader.initSparkContext(inputAdapters, outputAdapters, properties);
        }

        createSQLContext(jssc);
        streams = new BpsInputStreams();

        for (final BpsDataSourceConfiguration in : inputAdapters.getBpsDataSourceConfigurations()) {
            final Properties configuration = in.getDataSourceConfiguration();
            final String property = configuration.getProperty(Constants.URI);
            final URIDefinition<IOURIS> decode = IOURIS.decode(property);

            if (decode.getProtocol() != IOURIS.FILE) {
                throw new UnsupportedOperationException("Supports only File input and output operation in local mode.");
            }

            configuration.put(Constants.FILE_URL, decode.getContext());
            streams.add(in.getDataSourceContextName(),
                    new BpsBaseStream<DataFrame>(in.getDataSourceContextName(), createFileContext(configuration), configuration));
        }

        for (final BpsDataSinkConfiguration out : outputAdapters.getBpsDataSinkConfigurations()) {
            final BpsSparkFileDataSink<SQLContext> bpsSparkFileDataSink = new BpsSparkFileDataSink<>();
            bpsSparkFileDataSink.configureDataSink(context, out.getDataSinkConfiguration(), out.getDataSinkContextName());
            writers.add(bpsSparkFileDataSink);
        }

        LOG.trace("Existing the initialize method");
    }

    /**
     * Creates a DataFrame from a file and currently it supports text format.
     *
     * @param configuration
     *            the configuration
     * @return the data frame
     */
    private DataFrame createFileContext(final Properties configuration) {

        LOG.trace("Entering the createFileContext method");

        Preconditions.checkState(context != null, "Spark context is not initialized.");
        final String inferedSchema = configuration.getProperty(Constants.INFER_SCHEMA);
        final String header = configuration.getProperty(Constants.HEADER_ENABLED);
        final String tableName = configuration.getProperty("table-name");
        final boolean persist = Boolean.getBoolean(configuration.getProperty("persist"));
        final String partition = configuration.getProperty("partition.columns");
        final String format = configuration.getProperty(Constants.DATA_FORMAT);

        if (persist && !"text".equalsIgnoreCase(format)) {
            Preconditions.checkArgument(format != null && format.trim().length() != 0,
                    "Please specify a data format. The data.format supported values are text,parquet");
        }

        final DataFrame dataFrame = context.read().format("com.databricks.spark.csv").option("inferSchema", inferedSchema).option("header", header)
                .load(configuration.getProperty(Constants.URI));

        if (persist && !"text".equalsIgnoreCase(format)) {
            Preconditions.checkArgument(null != partition && partition.trim().length() > 0,
                    "If perisistance enabled for input, please provide one ore more partition colums comma seperated.");
            dataFrame.write().mode(SaveMode.Append).partitionBy(partition.split(",")).saveAsTable(tableName);
        } else {
            dataFrame.registerTempTable(tableName);
        }

        LOG.trace("Returning the createFileContext method");

        return dataFrame;
    }

    /**
     * Execute method runs Step of a pipeline job.
     */
    @Override
    public void execute() {

        LOG.trace("Entering the execute method");

        final Properties config = properties;
        final String property = config.getProperty("sql");
        final DataFrame sql = context.sql(property);

        for (final BpsSparkFileDataSink<SQLContext> fileWriter : writers) {
            fileWriter.write(sql);
        }

        LOG.trace("Existing the execute method");
    }

    /**
     * This operation will do the clean up for Job's Step handlers.
     */
    @Override
    public void cleanUp() {
        LOG.trace("Entering the cleanUp method");
        SparkAppFooter.closeSparkContext(jssc);
        jssc = null;
        context = null;
        LOG.trace("Existing the cleanUp method");
    }

    /**
     * This method creates a Spark SQL context.
     *
     * @param ctx
     *            the ctx
     */
    protected void createSQLContext(final JavaSparkContext ctx) {

        LOG.trace("Entering the createHiveContext method");

        if (context == null) {
            context = new SQLContext(ctx.sc());
            context.setConf("hive.exec.dynamic.partition", "true");
            context.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
        }

        LOG.trace("Existing the createHiveContext method");
    }

    /**
     * Gets the context.
     *
     * @return the context
     */
    public SQLContext getContext() {
        return context;
    }

    /**
     * Gets the java spark context.
     *
     * @return the jssc
     */
    public JavaSparkContext getJavaSparkContext() {
        return jssc;
    }

    /**
     * Gets the properties.
     *
     * @return the properties
     */
    public Properties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "LocalSparkSQLHandler [properties=" + PropertiesReader.getPropertyAsString(properties) + "]";
    }

    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_BATCH_LOCAL.getUri();
    }
}

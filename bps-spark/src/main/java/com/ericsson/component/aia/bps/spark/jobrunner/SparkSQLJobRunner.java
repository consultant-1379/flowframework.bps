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

import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.bps.core.service.configuration.BpsDataStreamsConfigurer;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.bps.core.service.streams.BpsOutputSinks;
import com.ericsson.component.aia.bps.core.utils.PropertiesReader;
import com.ericsson.component.aia.bps.spark.jobrunner.common.SparkAppFooter;
import com.ericsson.component.aia.bps.spark.jobrunner.common.SparkAppHeader;

/**
 * SparkSQLHandler class is a one of the implementation for Step interface. This handler is used when the user wants to run SQL query scenario using
 * Spark batch (With Hive Context).
 */
public class SparkSQLJobRunner implements BpsJobRunner {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(SparkSQLJobRunner.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 655833713003724650L;

    /** The context. */
    private HiveContext context;

    /** The jssc. */
    private JavaSparkContext jssc;

    /** The out going. */
    private BpsOutputSinks outGoing;

    /** The properties. */
    private Properties properties;

    /** The streams. */
    private transient BpsInputStreams bpsInputStreams;

    /**
     * This operation will do the clean up for Job's Step handlers.
     */
    @Override
    public void cleanUp() {
        LOG.trace("Entering the cleanUp method");
        outGoing.cleanUp();
        SparkAppFooter.closeSparkContext(jssc);
        jssc = null;
        context = null;
        LOG.trace("Existing the cleanUp method");
    }

    /**
     * This method creates a Spark hive context.
     *
     * @param ctx
     *            the ctx
     */
    protected void createHiveContext(final JavaSparkContext ctx) {
        LOG.trace("Entering the createHiveContext method");
        if (context == null) {
            context = new HiveContext(ctx.sc());
            context.setConf("hive.exec.dynamic.partition", "true");
            context.setConf("hive.exec.dynamic.partition.mode", "nonstrict");

        }
        LOG.trace("Existing the createHiveContext method");
    }

    /**
     * Execute method runs Step of a pipeline job.
     */
    @Override
    public void execute() {
        LOG.trace("Entering the execute method");
        final String sqlQuery = properties.getProperty("sql");
        final DataFrame sql = context.sql(sqlQuery);
        outGoing.write(sql);
        outGoing.cleanUp();
        getJavaSparkContext().stop();
        LOG.trace("Existing the execute method");
    }

    /**
     * Gets the context.
     *
     * @return the context
     */
    public HiveContext getContext() {
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
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_SQL.getUri();
    }

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
    @Override
    public void initialize(final BpsDataSourceAdapters inputAdapters, final BpsDataSinkAdapters outputAdapters, final Properties properties) {
        LOG.trace("Entering the initialize method");
        this.properties = properties;
        if (jssc == null) {
            jssc = SparkAppHeader.initSparkContext(inputAdapters, outputAdapters, properties);
        }
        createHiveContext(jssc);
        bpsInputStreams = BpsDataStreamsConfigurer.populateBpsInputStreams(inputAdapters, context);
        outGoing = BpsDataStreamsConfigurer.populateBpsOutputStreams(outputAdapters, context);
        LOG.trace("Existing the initialize method");
    }

    public BpsInputStreams getBpsInputStreams() {
        return bpsInputStreams;
    }

    @Override
    public String toString() {
        return "SparkSQLHandler [properties=" + PropertiesReader.getPropertyAsString(properties) + "]";
    }
}

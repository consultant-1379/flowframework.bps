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
package com.ericsson.component.aia.bps.flink.jobrunner;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.bps.core.service.configuration.BpsDataStreamsConfigurer;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.bps.core.service.streams.BpsOutputSinks;

/**
 * This class provide basic implementation to create data source and data sinks which the implementing class can use and implement transformation
 * logic
 */
public abstract class BpsFlinkStreamingJobRunner implements BpsJobRunner {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsFlinkStreamingJobRunner.class);

    protected Properties properties;

    private transient BpsInputStreams bpsInputStreams;

    private transient BpsOutputSinks bpsOutputSinks;

    private StreamExecutionEnvironment env;

    @Override
    public void execute() {
        try {
            executeJob();
            env.execute(this.getClass().getSimpleName());
            LOGGER.trace("execute() method is successful");
        } catch (final Exception exp) {
            LOGGER.error("execute failed !!", exp);
        }
    }

    /**
     * This method should be implemented by the implementing class to read input from data sources configured and provide custom logic for
     * transformations and write the return to data sinks
     */
    public abstract void executeJob();

    @Override
    public void initialize(final BpsDataSourceAdapters inputAdapters, final BpsDataSinkAdapters outputAdapters, final Properties properties) {
        this.properties = properties;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        LOGGER.trace("FlinkStreamingHandler props={}", properties);
        bpsInputStreams = BpsDataStreamsConfigurer.populateBpsInputStreams(inputAdapters, env);
        bpsOutputSinks = BpsDataStreamsConfigurer.populateBpsOutputStreams(outputAdapters, env);
    }

    /**
     * This method will write the given {@link DataStream} into all the configured data sinks
     *
     * @param record
     *            to write into all configured data sinks
     */
    public void persistDataStream(final DataStream<?> record) {
        LOGGER.trace("Record null returning");
        if (record == null) {
            return;
        }
        LOGGER.trace("Record not null writing now...");
        bpsOutputSinks.write(record);
    }

    public void setStreamExecutionEnvironment(final StreamExecutionEnvironment env) {
        this.env = env;
    }

    protected BpsInputStreams getBpsInputStreams() {
        return bpsInputStreams;
    }

    protected StreamExecutionEnvironment getEnv() {
        return env;
    }

    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.FLINK_STREAMING.getUri();
    }
}

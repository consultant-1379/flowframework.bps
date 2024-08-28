/**
 *
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 *
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 *
 */
package com.ericsson.component.aia.bps.core.service.configuration;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.BpsDataSinkService;
import com.ericsson.component.aia.bps.core.service.BpsDataSourceService;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceConfiguration;
import com.ericsson.component.aia.bps.core.service.streams.BpsBaseStream;
import com.ericsson.component.aia.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.bps.core.service.streams.BpsOutputSinks;
import com.ericsson.component.aia.common.service.loader.GenericServiceLoader;

/**
 * This class provides api's to configure bps I/O streams for given CONTEXT type and configurations .
 */
public final class BpsDataStreamsConfigurer {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsDataStreamsConfigurer.class);

    private BpsDataStreamsConfigurer() {
    }

    /**
     *
     * This method iterates over {@link BpsDataSourceConfiguration}'s wrapped in {@link BpsDataSourceAdapters} parameter and create bps data source
     * {@link BpsBaseStream} of type OUT on CONTEXT type provided as parameter
     *
     * @param <CONTEXT>
     *            of the data source
     *
     * @param <OUT>
     *            data source output type
     * @param dataSourceAdapters
     *            containing list of {@link BpsDataSourceConfiguration} for creating Input streams on CONTEXT type provided as parameter
     * @param context
     *            to attach Input streams created on CONTEXT type provided as parameter
     * @return {@link BpsInputStreams} containing Input streams created on CONTEXT type provided
     */
    @SuppressWarnings("unchecked")
    public static <CONTEXT, OUT> BpsInputStreams populateBpsInputStreams(final BpsDataSourceAdapters dataSourceAdapters, final CONTEXT context) {
        LOG.trace("Entering the addStream method ");
        final BpsInputStreams streams = new BpsInputStreams();
        for (final BpsDataSourceConfiguration in : dataSourceAdapters.getBpsDataSourceConfigurations()) {
            try {
                final Properties configuration = in.getDataSourceConfiguration();
                final String uri = IOURIS.getURIS(configuration).getUri();
                final String dataSourceContextName = in.getDataSourceContextName();
                final BpsDataSourceService<CONTEXT, OUT> dataStreamSource = (BpsDataSourceService<CONTEXT, OUT>) GenericServiceLoader
                        .getService(BpsDataSourceService.class, uri);
                dataStreamSource.configureDataSource(context, configuration, dataSourceContextName);
                streams.add(dataSourceContextName, new BpsBaseStream<OUT>(dataSourceContextName, dataStreamSource.getDataStream()));
            } catch (final Exception exp) {
                System.out.println(
                        String.format("Exception occurred while adding data source=%s , reason=", in.getDataSourceContextName(), exp.getMessage()));
                exp.printStackTrace();
            }
        }
        LOG.trace("Returning the addStream method ");
        return streams;
    }

    /**
     * This method iterates over {@link BpsDataSinkConfiguration} wrapped in {@link BpsDataSinkAdapters} parameter and create bps data sinks
     * {@link BpsDataSinkService} of type OUT on CONTEXT type provided as parameter
     *
     * @param <CONTEXT>
     *            of the data sink
     *
     * @param <OUT>
     *            data sink output type
     *
     * @param dataSinkAdapters
     *            containing list of {@link BpsDataSinkConfiguration} for creating Input streams on CONTEXT type provided as parameter
     * @param context
     *            to attach bps data sinks created on CONTEXT type provided as parameter
     * @return {@link BpsOutputSinks} containing output data sinks created on CONTEXT type provided
     */
    @SuppressWarnings("unchecked")
    public static <CONTEXT, OUT> BpsOutputSinks populateBpsOutputStreams(final BpsDataSinkAdapters dataSinkAdapters, final CONTEXT context) {
        LOG.trace("Entering the addStream method ");
        final BpsOutputSinks dataSinks = new BpsOutputSinks();
        for (final BpsDataSinkConfiguration out : dataSinkAdapters.getBpsDataSinkConfigurations()) {
            final Properties configuration = out.getDataSinkConfiguration();
            final String uri = IOURIS.getURIS(configuration).getUri();
            final BpsDataSinkService<CONTEXT, OUT> dataSinkService = (BpsDataSinkService<CONTEXT, OUT>) GenericServiceLoader
                    .getService(BpsDataSinkService.class, uri);
            dataSinkService.configureDataSink(context, configuration, out.getDataSinkContextName());
            dataSinks.addBpsDataSinkService(dataSinkService);
        }
        LOG.trace("Returning the addStream method ");
        return dataSinks;
    }
}
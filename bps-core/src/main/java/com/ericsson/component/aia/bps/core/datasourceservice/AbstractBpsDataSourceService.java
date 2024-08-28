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
package com.ericsson.component.aia.bps.core.datasourceservice;

import java.util.Properties;

import com.ericsson.component.aia.bps.core.service.BpsDataSourceService;

/**
 * The <code>AbstractBpsDataSourceService</code> provides basic implementation required for <code>BpsDataSourceService</code>. This class needs to be
 * extend by all the specific implementation of BpsDataSourceService.
 *
 * @param <C>
 *            the generic type representing the context like {@link HiveContext } etc.
 * @param <O>
 *            the generic type representing the output like {@link DataFrame } etc.
 */
public abstract class AbstractBpsDataSourceService<C, O> implements BpsDataSourceService<C, O> {

    /**
     * Context associated with input data source.
     */
    protected C context;

    /**
     * Property associated with configuration.
     */
    protected Properties properties;

    /**
     * Name associated with input dataSourceContextName.
     */
    protected String dataSourceContextName;

    /**
     * Configured specific instance which extends {@link AbstractBpsDataSourceService} for the specified dataSourceContextName.<br>
     * dataSourceContextName is the name of the input data source which was configured in flow.xml through tag &lt;input
     * name="dataSourceContextName"&gt; .... &lt;/input&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param properties
     *            the configuration associated with underlying output sink.
     * @param dataSourceContextName
     *            Unique name associated with each of the output sink.
     */
    @Override
    public void configureDataSource(final C context, final Properties properties, final String dataSourceContextName) {
        this.context = context;
        this.properties = properties;
        this.dataSourceContextName = dataSourceContextName;
    }

    /**
     * will return data source context name associated with it.
     */
    @Override
    public String getDataSourceContextName() {
        return dataSourceContextName;
    }
}

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
package com.ericsson.component.aia.bps.core.service;

import java.util.Properties;

import com.ericsson.component.aia.common.service.GenericService;

/**
 * This interface represents the Bps Data source service. All implementation should implement this interface for provider specific logic.
 *
 * @param <C>
 *            Context of the Bps Data source service
 * @param <O>
 *            data type to read
 */
public interface BpsDataSourceService<C, O> extends GenericService {

    /**
     * This method configures bps data source service.
     *
     * @param context
     *            of the bps data source
     * @param properties
     *            of the bps data source
     * @param dataSourceContextName
     *            unique name associated with bps data source
     */
    void configureDataSource(C context, Properties properties, String dataSourceContextName);

    /**
     * This method returns unique name associated with bps data source.
     *
     * @return unique name associated with bps data source
     */
    String getDataSourceContextName();

    /**
     * This method will create and return input stream of type OUT based on
     * {@link BpsDataSourceService#configureDataSource(Object, Properties, String)}.
     *
     * @return data stream of type OUT
     */
    O getDataStream();
}

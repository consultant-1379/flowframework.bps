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
 * This interface represents the Bps Data sink service. All implementation should implement this interface for provide provider specific logic
 *
 * @param <C>
 *            Context of the Bps Data sink service
 * @param <O>
 *            data type to be sinked
 */
public interface BpsDataSinkService<C, O> extends GenericService {

    /**
     * This method configures bps data sink service.
     *
     * @param context
     *            of the bps data sink
     * @param properties
     *            of the bps data sink
     * @param dataSinkContextName
     *            unique name associated with bps data sink
     */
    void configureDataSink(C context, Properties properties, String dataSinkContextName);

    /**
     * Write operation writes bytes to output binary stream.
     *
     * @param dataStream
     *            the data stream
     */
    void write(O dataStream);

    /**
     * This operation will do the clean up for writers.
     */
    void cleanUp();

    /**
     * This method returns Bps data sink context name.
     *
     * @return bps data sink context name
     */
    String getDataSinkContextName();
}

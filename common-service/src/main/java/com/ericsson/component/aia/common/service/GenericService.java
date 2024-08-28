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
package com.ericsson.component.aia.common.service;

import com.ericsson.component.aia.common.service.loader.GenericServiceLoader;

/**
 * This is Generic service. This needs to be followed by all the implementation who needs SPI support from {@link GenericServiceLoader}
 */
public interface GenericService {
    /**
     * This method will return unique context associated with SPI.
     *
     * @return service context name.
     */
    String getServiceContextName();
}

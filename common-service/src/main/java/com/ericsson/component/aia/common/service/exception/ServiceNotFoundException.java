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
package com.ericsson.component.aia.common.service.exception;

import com.ericsson.component.aia.common.service.loader.GenericServiceLoader;

/**
 * Exception class thrown when no service implementation is found by the {@link GenericServiceLoader}
 */
public class ServiceNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 6896106995131693495L;

    /**
     * @param message
     *            to describe the exception scenario
     */
    public ServiceNotFoundException(final String message) {
        super(message);
    }

    /**
     * @param message
     *            exception message
     * @param cause
     *            throwable cause
     */
    public ServiceNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }
}

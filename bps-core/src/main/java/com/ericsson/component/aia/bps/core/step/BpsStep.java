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
package com.ericsson.component.aia.bps.core.step;

import java.util.Properties;

import com.ericsson.component.aia.bps.core.service.BpsJobRunner;

/**
 * Step is execution unit of the job.
 *
 */
public interface BpsStep {

    /**
     * Configure Step properties.
     *
     * @param name
     *            the name
     * @param uri
     *            String uri
     * @return boolean true is the URI is known otherwise false
     */
    boolean configure(String name, Properties uri);

    /**
     * Gets the configuration.
     *
     * @return the configuration
     */
    Properties getconfiguration();

    /**
     * Gets the name.
     *
     * @return the name
     */
    String getName();

    /**
     * Execute operation runs step job.
     */
    void execute();

    /**
     * Sets the step handler.
     *
     * @param stepHandler
     *            the new step handler
     */
    void setStepHandler(BpsJobRunner stepHandler);

    /**
     * Gets the step handler.
     *
     * @return the step handler
     */
    BpsJobRunner getStepHandler();

    /**
     * This method will cleanup all the resources allocated for this step.
     */
    void cleanUp();
}

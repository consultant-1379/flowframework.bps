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
package com.ericsson.component.aia.bps.core.service;

import java.io.Serializable;
import java.util.Properties;

import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.common.service.GenericService;

/**
 * JobRunner interface runs pipeline jobs based on the flow plan.
 */
public interface BpsJobRunner extends GenericService, Serializable {

    /**
     * Execute method runs step of a pipeline job.
     */
    void execute();

    /**
     * Initializes pipeline parameters.
     *
     * @param inputAdapters
     *            the input adapters
     * @param outputAdapters
     *            the output adapters
     * @param properties
     *            the properties
     */
    void initialize(BpsDataSourceAdapters inputAdapters, BpsDataSinkAdapters outputAdapters, Properties properties);

    /**
     * This operation will do the clean up for Job runners.
     */
    void cleanUp();
}

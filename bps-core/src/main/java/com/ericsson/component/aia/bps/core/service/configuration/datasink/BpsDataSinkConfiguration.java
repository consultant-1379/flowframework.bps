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
package com.ericsson.component.aia.bps.core.service.configuration.datasink;

import java.io.Serializable;
import java.util.Properties;

/**
 * OutputStreamConfiguration is a base interface for all Bps data sinks. Each implementation of this interface is meant to provide their own version
 * of Output data sink and creating a connection for writing the Output.
 *
 * The OutputStreamConfiguration interface provides operations to create different data sinks.
 */
public interface BpsDataSinkConfiguration extends Serializable {

    /**
     * Configure properties for Bps data sink service
     *
     * @param dataSinkContextName
     *            the bps data sink context name
     * @param properties
     *            the bps data sink configuration
     */
    void configure(String dataSinkContextName, Properties properties);

    /**
     * Gets the Bps data sink configuration.
     *
     * @return the configuration
     */
    Properties getDataSinkConfiguration();

    /**
     * Gets the OutputStream name.
     *
     * @return the name
     */
    String getDataSinkContextName();

}

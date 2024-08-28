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
package com.ericsson.component.aia.bps.core.service.configuration.datasource;

import java.io.Serializable;
import java.util.Properties;

/**
 * <code>InputStreamConfiguration</code> is a base interface for all Bps Input data source. Each implementation of this interface is meant to provide
 * their own version of Input data sources and creating a connection for reading the Input.
 *
 * The InputStreamConfiguration interface provides operations to create different Input data source channels.
 */
public interface BpsDataSourceConfiguration extends Serializable {

    /**
     * Configure properties for InputStream to registry in Bps.
     *
     * @param dataSourceContextName
     *            the name
     * @param properties
     *            Properties uri
     */
    void configure(String dataSourceContextName, Properties properties);

    /**
     * Gets the InputStream name.
     *
     * @return the name
     */
    String getDataSourceContextName();

    /**
     * Gets the InputStream configuration.
     *
     * @return the configuration
     */
    Properties getDataSourceConfiguration();
}

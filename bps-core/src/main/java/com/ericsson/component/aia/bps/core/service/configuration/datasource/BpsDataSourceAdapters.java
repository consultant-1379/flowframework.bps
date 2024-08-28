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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BpsDataSourceAdapters} class holds the collection of {@link BpsDataSourceConfiguration}, which are configured through flow.xml. <br>
 * Flow.xml provides a tag &lt;input name="input_stream_config"&gt &lt;/input&gt; which was used by pipe builder to build the list of inputs.
 */
public class BpsDataSourceAdapters implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(BpsDataSourceAdapters.class);

    /** Generated version id for this file. */
    private static final long serialVersionUID = 5302116426574913781L;

    /** Collection of Inputstreams from flow xml. */
    private final List<BpsDataSourceConfiguration> bpsDataSourceConfigurations = new ArrayList<>();

    /**
     * Gets all InputStreams configured as a part of flow xml.
     *
     * @return the input streams
     */
    public List<BpsDataSourceConfiguration> getBpsDataSourceConfigurations() {
        return bpsDataSourceConfigurations;
    }

    /**
     * AddInputStream method adds a adapter to Bps.
     *
     * @param dataSourceConfiguration
     *            Input Adapter
     */
    public void addBpsDataSourceConfiguration(final BpsDataSourceConfiguration dataSourceConfiguration) {
        LOG.trace("Adding a InputStream " + dataSourceConfiguration.getDataSourceContextName());
        bpsDataSourceConfigurations.add(dataSourceConfiguration);
        LOG.trace("Successfully add InputStream " + dataSourceConfiguration.getDataSourceContextName());
    }

    @Override
    public String toString() {
        return "InputStreamAdapters [inputStreams=" + bpsDataSourceConfigurations + "]";
    }
}

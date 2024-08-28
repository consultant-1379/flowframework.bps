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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>BpsDataSinkAdapters</code> class holds the collection of {@link BpsDataSinkConfiguration}, which are configured through flow.xml. <br>
 * Flow.xml provides a tag &lt;input name="output_stream_config"&gt &lt;/input&gt; which was used by pipe builder to build the list of inputs.
 */
public final class BpsDataSinkAdapters implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(BpsDataSinkAdapters.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -3948755104962602371L;

    /** Collection of OutputStreams from flow xml. */
    private final List<BpsDataSinkConfiguration> bpsDataSinkConfigurations = new ArrayList<>();

    /**
     * Gets all BpsDataSinkConfiguration configured as a part of flow xml.
     *
     * @return the output streams
     */
    public List<BpsDataSinkConfiguration> getBpsDataSinkConfigurations() {
        return bpsDataSinkConfigurations;
    }

    /**
     * addOutputStream method adds BpsDataSinkConfiguration to Bps.
     *
     * @param dataSinkConfiguration
     *            the bps data sink configuration
     */
    public void addBpsDataSinkConfiguration(final BpsDataSinkConfiguration dataSinkConfiguration) {
        LOG.trace("Adding a Bps data sink configuration " + dataSinkConfiguration.getDataSinkContextName());
        bpsDataSinkConfigurations.add(dataSinkConfiguration);
        LOG.trace("Successfully added Bps data sink configuration " + dataSinkConfiguration.getDataSinkContextName());
    }

    @Override
    public String toString() {
        return "BpsDataSinkAdapters [BpsDataSinkConfigurations=" + bpsDataSinkConfigurations + "]";
    }
}

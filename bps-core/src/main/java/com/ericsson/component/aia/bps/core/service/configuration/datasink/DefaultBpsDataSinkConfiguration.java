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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants.ConfigType;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.utils.IOUtil;
import com.ericsson.component.aia.bps.core.validation.BpsConfigurationValidation;

/**
 * DefaultOutputs class holds/refer different bps data source sinks.
 */
public class DefaultBpsDataSinkConfiguration extends BpsConfigurationValidation implements BpsDataSinkConfiguration {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBpsDataSinkConfiguration.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The context name. */
    private String dataSinkContextName;

    /** The properties. */
    private Properties properties;

    /**
     * configure method initializes different OutputStreams after mandatory fields check for a stream.
     *
     * @param dataSinkContextName
     *            the data sink context name
     * @param props
     *            the props
     */
    @Override
    public void configure(final String dataSinkContextName, final Properties props) {
        LOG.trace("Entering the configure method");
        final Set<String> requiredProps = IOUtil.getMandatoryProps(dataSinkContextName, props, ConfigType.OUTPUT);

        checkArgument(requiredProps != null, "No configuration done for the Output adapter " + IOURIS.getUriType(props));

        checkForMandatoryProps(requiredProps, props);
        this.properties = props;
        this.dataSinkContextName = dataSinkContextName;
        LOG.trace("Existing the configure method");
    }

    @Override
    public String getDataSinkContextName() {
        return dataSinkContextName;
    }

    @Override
    public Properties getDataSinkConfiguration() {
        return properties;
    }

    @Override
    public String toString() {
        return "[OutputName=" + IOURIS.getUriType(properties) + " contextName=" + dataSinkContextName + "]";
    }
}

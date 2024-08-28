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

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.AttributeValueType;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.InputOutputType;
import com.google.common.base.Preconditions;

/**
 * A factory class for creating Outputstream objects.
 */
public class BpsDataSinkConfigurationFactory {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsDataSinkConfigurationFactory.class);

    private BpsDataSinkConfigurationFactory() {

    }

    /**
     * Creates OutputStream based on the configuration.
     *
     * @param inputOutputType
     *            the input output type
     * @return the output stream
     */
    public static BpsDataSinkConfiguration create(final InputOutputType inputOutputType) {
        LOG.trace("Entering the create method");
        final Properties props = new Properties();
        final String name = inputOutputType.getName();
        final List<AttributeValueType> attribute = inputOutputType.getAttribute();
        props.put("output.name", name);

        for (final AttributeValueType attributeValueType : attribute) {
            props.put(attributeValueType.getName(), attributeValueType.getValue());
        }

        final String uri = props.getProperty(Constants.URI);
        Preconditions.checkArgument(uri != null && uri.trim().length() > 0, "Could not locate a valid uri, please check the Flow file.");

        final DefaultBpsDataSinkConfiguration uriOutput = new DefaultBpsDataSinkConfiguration();
        uriOutput.configure(name, props);
        LOG.trace("Existing the create method");
        return uriOutput;
    }
}

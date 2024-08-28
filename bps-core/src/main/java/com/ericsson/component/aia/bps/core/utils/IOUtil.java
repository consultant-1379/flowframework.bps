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
package com.ericsson.component.aia.bps.core.utils;

import static com.ericsson.component.aia.bps.core.common.Constants.DOT;

import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.Constants.ConfigType;
import com.google.common.base.Preconditions;

/**
 * IOUtil is utility class which consists of commons methods.
 */
public class IOUtil {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(IOUtil.class);

    private IOUtil() {

    }

    /**
     * Gets the mandatory props for Step, Input & Output adapters.
     *
     * @param name
     *            the name
     * @param props
     *            the props
     * @param configType
     *            the config type
     * @return the mandatory props
     */
    public static Set<String> getMandatoryProps(final String name, final Properties props, final ConfigType configType) {
        LOG.trace("Entering the getMandatoryProps method");

        Preconditions.checkArgument(props != null, name + " Stream connection properties cannot be null.");
        String uri = props.getProperty(Constants.URI);
        uri = uri.substring(0, uri.indexOf(":"));
        Preconditions.checkArgument(uri != null, "URI cannot be null.");

        // do validation
        return PropertiesReader.getInstance().getConfigs(configType.getPath() + DOT + uri);
    }
}

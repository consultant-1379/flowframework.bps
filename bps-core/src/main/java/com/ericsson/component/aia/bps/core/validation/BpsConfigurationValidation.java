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
package com.ericsson.component.aia.bps.core.validation;

import static com.google.common.base.Preconditions.*;

import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConfigurationValidation checks whether Step, Input or Output configurations are valid or not.
 */
public abstract class BpsConfigurationValidation {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsConfigurationValidation.class);

    /**
     * Validate operation checks whether mandatory properties are present or not.
     *
     * @param property
     *            the property
     * @param props
     *            the props
     */
    private void validate(final String property, final Properties props) {
        LOG.trace("Entering the ConfigurationValidation validate");
        checkArgument(props.getProperty(property) != null, "Require property " + property + "  cannot be null");
        LOG.trace("Existing the ConfigurationValidation validate");
    }

    /**
     * Check for mandatory properties.
     *
     * @param requiredProperties
     *            the required properties
     * @param props
     *            the props
     */
    protected void checkForMandatoryProps(final Set<String> requiredProperties, final Properties props) {
        LOG.trace("Entering the ConfigurationValidation checkForMandatoryProps");
        for (final String key : requiredProperties) {
            validate(key, props);
        }
        LOG.trace("Existing the ConfigurationValidation checkForMandatoryProps");
    }
}

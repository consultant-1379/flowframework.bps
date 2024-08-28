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
package com.ericsson.component.aia.bps.core.step;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.bps.core.validation.BpsConfigurationValidation;
import com.google.common.base.Preconditions;

/**
 * DefaultStep class holds/refer different Steps.
 */
public class BpsDefaultStep extends BpsConfigurationValidation implements BpsStep {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsDefaultStep.class);

    /** The properties. */
    private Properties properties;

    /** The context. */
    private String context;

    /** The step handler. */
    private BpsJobRunner stepHandler;

    /**
     * configure method initializes different Step after mandatory fields check for a pipe.
     *
     * @param contextName
     *            the context name
     * @param props
     *            the props
     * @return true, if successful
     */
    @Override
    public boolean configure(final String contextName, final Properties props) {
        LOG.trace("Entering the configure method");
        checkArgument(props != null, contextName + " Stream connection properties cannot be null.");
        String uri = props.getProperty(Constants.URI);
        uri = uri.substring(0, uri.indexOf(":"));
        Preconditions.checkArgument(uri != null, "URI cannot be null.");
        // do validation
        //        final Set<String> requiredProps = PropertiesReader.getInstance().getConfigs(uri);

        //        checkArgument(requiredProps != null, "No configuration done for the Step " + PROCESS_URIS.getUriType(props));
        //        checkForMandatoryProps(requiredProps, props);
        this.properties = props;
        this.context = contextName;
        LOG.trace("Existing the configure method");
        return true;
    }

    /**
     * Execute method run a Step.
     */
    @Override
    public void execute() {
        LOG.trace("Entering the execute method");
        if (null == stepHandler) {
            throw new IllegalArgumentException("Missing Step handler for " + context);
        }
        stepHandler.execute();
        LOG.trace("Existing the execute method");
    }

    /**
     * This operation will do the clean up for Step.
     */
    @Override
    public void cleanUp() {
        LOG.trace("Entering the cleanUp method");
        if (null == stepHandler) {
            throw new IllegalArgumentException("Missing Step handler for " + context);
        }
        stepHandler.cleanUp();
        LOG.trace("Existing the cleanUp method");
    }

    @Override
    public Properties getconfiguration() {
        return properties;
    }

    @Override
    public String getName() {
        return context;
    }

    /**
     * Gets the properties.
     *
     * @return the properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the properties.
     *
     * @param properties
     *            the new properties
     */
    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "[StepName=" + PROCESS_URIS.getUriType(properties) + " contextName=" + context + "]";
    }

    @Override
    public BpsJobRunner getStepHandler() {
        return stepHandler;
    }

    @Override
    public void setStepHandler(final BpsJobRunner stepHandler) {
        this.stepHandler = stepHandler;
    }
}

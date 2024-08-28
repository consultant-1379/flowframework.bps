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

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.common.service.loader.GenericServiceLoader;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.AttributeValueType;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.StepType;
import com.google.common.base.Preconditions;

/**
 * A factory for creating Step objects.
 */
public class BpsStepFactory implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 2854654812257328592L;

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsStepFactory.class);

    private BpsStepFactory() {

    }

    /**
     * Creates the Step based on the configuration.
     *
     * @param stepType
     *            the step type
     * @return the step
     */
    public static BpsStep create(final StepType stepType) {
        LOG.trace("Entering the create method");
        final Properties props = new Properties();
        final String name = stepType.getName();
        final List<AttributeValueType> attribute = stepType.getAttribute();
        props.put("step.name", name);

        for (final AttributeValueType attributeValueType : attribute) {
            props.put(attributeValueType.getName(), attributeValueType.getValue());
        }
        final String uri = props.getProperty(Constants.URI);
        Preconditions.checkArgument(uri != null && uri.trim().length() > 0, "Could not locate a valid uri, please check the Flow file.");

        final BpsDefaultStep defaultStep = new BpsDefaultStep();
        defaultStep.configure(name, props);
        defaultStep.setStepHandler(getStepHandler(props));
        LOG.trace("Existing the create method");
        return defaultStep;
    }

    /**
     * Gets the step handler.
     *
     * @param props
     *            the props
     * @return the step handler
     */
    private static BpsJobRunner getStepHandler(final Properties props) {
        final PROCESS_URIS uri = PROCESS_URIS.getURIS(props);
        return (BpsJobRunner) GenericServiceLoader.getService(BpsJobRunner.class, uri.getUri());
    }

}

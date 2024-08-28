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
package com.ericsson.component.aia.bps.engine.parser;

import static java.lang.String.format;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Collection;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.pipe.BpsPipe;
import com.ericsson.component.aia.bps.core.pipe.BpsPipelineRunner;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.FlowDefinition;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.util.MetaModelConstants;

/**
 * This class parses flow xml and builds pipe-line based on flow xml configuration.
 */
final class BpsModelParser implements Serializable {

    private static final long serialVersionUID = 580185855482292834L;
    private static final Logger LOG = LoggerFactory.getLogger(BpsModelParser.class);

    private BpsModelParser() {
    }

    /**
     * This method will try to find the <Param>modelType</Param> in the application context and creates JAXB <Code>Unmarshaller</Code>
     *
     * @param modelType
     *            of the flow xml schema to load from application context
     * @return configured <Code>Unmarshaller</Code> to read xml based on <Param>modelType</Param> schema
     * @throws JAXBException
     */
    private static Unmarshaller getUnmarshaller(final String modelType) throws JAXBException {
        LOG.debug(format("Trying to find unmarshaller for %s", modelType));
        final Collection<String> dependencies = MetaModelConstants.getDependenciesFor(modelType);

        final StringBuilder contextPath = new StringBuilder();
        contextPath.append(MetaModelConstants.getJavaPackageFor(modelType));

        for (final String dependency : dependencies) {
            contextPath.append(':');
            contextPath.append(MetaModelConstants.getJavaPackageFor(dependency));
        }
        LOG.debug("Trying to find unmarshaller for " + modelType);
        final JAXBContext jaxbContext = JAXBContext.newInstance(contextPath.toString());
        final Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        unmarshaller.setSchema(null);
        return unmarshaller;
    }

    /**
     * This methods accepts {@link InputStream} of flow xml and builds the pipe-line using flow xml configuration and returns the same for execution
     *
     * @param inputStream
     *            of flow xml with configuration to build pipe-line
     * @return configured pipe line
     */
    public static BpsPipe parseFlow(final InputStream inputStream) {
        try {
            final Unmarshaller unmarshaller = getUnmarshaller(MetaModelConstants.FBP_FLOW);
            final Object root = unmarshaller.unmarshal(new StreamSource(inputStream));
            final FlowDefinition flowDefinition = (FlowDefinition) root;
            final BpsPipe runner = BpsPipelineRunner.Builder.create().addFlow(flowDefinition).build();
            return runner;
        } catch (final JAXBException jaxbexc) {
            throw new IllegalStateException("Invalid module - unable to parse it! Details: " + jaxbexc);
        }
    }

}

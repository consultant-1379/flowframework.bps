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
package com.ericsson.component.aia.bps.core.pipe;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkConfigurationFactory;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceConfiguration;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceConfigurationFactory;
import com.ericsson.component.aia.bps.core.step.BpsStep;
import com.ericsson.component.aia.bps.core.step.BpsStepFactory;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.FlowDefinition;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.InputOutputType;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.PathType;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.StepType;
import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.ToType;

/**
 * PipelineRunner executes the operations present in the pipeline through Steps.
 */
public class BpsPipelineRunner implements BpsPipe {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsPipelineRunner.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1666289193387607910L;

    /** The service. */
    private static ServiceLoader<BpsJobRunner> service;

    /** The ip adapters. */
    private BpsDataSourceAdapters ipAdapters;

    /** The op adapters. */
    private BpsDataSinkAdapters opAdapters;

    /** The pipe. */
    private List<PathType> pipe;

    /** The task seq. */
    private final List<String> taskSeq = new ArrayList<>();

    /** The name. */
    private String name;

    /** The version. */
    private String version;

    /** The namespace. */
    private String namespace;

    /** The path. */
    private final List<PathType> path;

    /** The steps. */
    private final List<BpsStep> steps;

    /**
     * Instantiates a new pipeline runner.
     *
     * @param builder
     *            the builder
     */
    private BpsPipelineRunner(final Builder builder) {
        this.name = builder.name;
        this.version = builder.version;
        this.namespace = builder.namespace;
        this.path = builder.path;
        this.steps = builder.steps;
    }

    /**
     * The Class Builder.
     */
    public static class Builder {

        /** The name. */
        private String name;

        /** The version. */
        private String version;

        /** The namespace. */
        private String namespace;

        /** The path. */
        private List<PathType> path;

        /** The ip adapters. */
        private BpsDataSourceAdapters ipAdapters;

        /** The op adapters. */
        private BpsDataSinkAdapters opAdapters;

        /** The steps. */
        private List<BpsStep> steps;

        /**
         * Creates the.
         *
         * @return the builder
         */
        public static Builder create() {
            return new Builder();
        }

        /**
         * Adds the flow and creates a builder object.
         *
         * @param flowDefinition
         *            the flow definition
         * @return the builder
         */
        public Builder addFlow(final FlowDefinition flowDefinition) {
            LOG.trace("Entering the addFlow method");
            checkArgument(flowDefinition != null, "Flow cannot be null.");

            return addName(flowDefinition.getName()).addNamespace(flowDefinition.getNs()).addVersion(flowDefinition.getVersion())
                    .addPathType(flowDefinition.getPath()).addInputs(flowDefinition.getInput()).addOutputs(flowDefinition.getOutput())
                    .addSteps(flowDefinition.getStep());
        }

        /**
         * Adds the name.
         *
         * @param name
         *            the name
         * @return the builder
         */
        private Builder addName(final String name) {
            checkArgument(name != null, "FlowDefinition Name cannot be null.");
            this.name = name;
            LOG.trace("Existing the addName method");
            return this;
        }

        /**
         * Adds the namespace.
         *
         * @param namespace
         *            the namespace
         * @return the builder
         */
        private Builder addNamespace(final String namespace) {
            checkArgument(namespace != null, "Namespace can not be null.");
            this.namespace = namespace;
            return this;
        }

        /**
         * Adds the version.
         *
         * @param version
         *            the version
         * @return the builder
         */
        private Builder addVersion(final String version) {
            checkArgument(version != null, "Version can not be null.");
            this.version = version;
            return this;
        }

        /**
         * Adds the path type.
         *
         * @param path
         *            the path
         * @return the builder
         */
        private Builder addPathType(final List<PathType> path) {
            checkArgument(path != null && !path.isEmpty(), "Path's can not be null or empty.");
            this.path = path;
            return this;
        }

        /**
         * Adds the inputs.
         *
         * @param inputList
         *            the input list
         * @return the builder
         */
        private Builder addInputs(final List<InputOutputType> inputList) {
            LOG.trace("Entering the addInputs method");
            checkArgument(inputList != null && !inputList.isEmpty(), "Inputs can not be null or empty.");
            ipAdapters = new BpsDataSourceAdapters();
            for (final InputOutputType inputOutputType : inputList) {
                ipAdapters.addBpsDataSourceConfiguration(BpsDataSourceConfigurationFactory.create(inputOutputType));
            }
            LOG.trace("Existing the addInputs method");
            return this;
        }

        /**
         * Adds the outputs.
         *
         * @param outputList
         *            the output list
         * @return the builder
         */
        private Builder addOutputs(final List<InputOutputType> outputList) {
            LOG.trace("Entering the addOutputs method");
            checkArgument(outputList != null && !outputList.isEmpty(), "Outputs can not be null or empty.");
            opAdapters = new BpsDataSinkAdapters();
            for (final InputOutputType inputOutputType : outputList) {
                opAdapters.addBpsDataSinkConfiguration(BpsDataSinkConfigurationFactory.create(inputOutputType));
            }
            LOG.trace("Existing the addOutputs method");
            return this;
        }

        /**
         * Adds the steps.
         *
         * @param steps
         *            the steps
         * @return the builder
         */
        private Builder addSteps(final List<StepType> steps) {
            LOG.trace("Entering the addSteps method");
            checkArgument(steps != null && !steps.isEmpty(), "Steps can not be null or empty.");
            if (steps.size() > 1) {
                throw new IllegalArgumentException("Currently only supporting single processing step");
            }
            this.steps = new LinkedList<BpsStep>();
            for (final StepType stepType : steps) {
                final BpsStep step = BpsStepFactory.create(stepType);
                step.getStepHandler().initialize(ipAdapters, opAdapters, step.getconfiguration());
                this.steps.add(step);
            }
            LOG.trace("Existing the addSteps method");
            return this;
        }

        /**
         * Checks pipeline is valid or not.
         */
        private void checkRequired() {
            LOG.trace("Entering the checkRequired method");
            checkArgument(name != null, "FlowDefinition Name cannot be null.");
            checkArgument(namespace != null, "Namespace can not be null.");
            checkArgument(version != null, "Version can not be null.");
            checkArgument(path != null && !path.isEmpty(), "Path's can not be null or empty.");
            checkArgument(ipAdapters != null && !ipAdapters.getBpsDataSourceConfigurations().isEmpty(), "Inputs can not be null or empty.");
            checkArgument(opAdapters != null && !opAdapters.getBpsDataSinkConfigurations().isEmpty(), "Outputs can not be null or empty.");
            checkArgument(steps != null && !steps.isEmpty(), "Steps can not be null or empty.");
            LOG.trace("Existing the checkRequired method");
        }

        /**
         * Builds the pipe.
         *
         * @return the pipe
         */
        public BpsPipe build() {
            LOG.trace("Entering the build method");
            checkRequired();
            LOG.trace("Existing the build method");
            return new BpsPipelineRunner(this);
        }
    }

    /**
     * TODO: Place holder for future to merge two inputs.
     *
     * @param ipAdapters
     *            the ip adapters
     * @param step
     *            the step
     * @param opAdapters
     *            the op adapters
     * @return the properties
     */
    @SuppressWarnings("unused")
    private Properties joinPropertiesByContext(final BpsDataSourceAdapters ipAdapters, final BpsStep step, final BpsDataSinkAdapters opAdapters) {
        LOG.trace("Entering the joinPropertiesByContext method");
        final Properties propertyAll = new Properties();
        final List<BpsDataSourceConfiguration> inputCollection = ipAdapters.getBpsDataSourceConfigurations();
        for (final BpsDataSourceConfiguration bpsInput : inputCollection) {
            mapProperties(propertyAll, bpsInput.getDataSourceContextName(), bpsInput.getDataSourceConfiguration());
        }
        mapProperties(propertyAll, step.getName(), step.getconfiguration());
        for (final BpsDataSinkConfiguration out : opAdapters.getBpsDataSinkConfigurations()) {
            mapProperties(propertyAll, out.getDataSinkContextName(), out.getDataSinkConfiguration());
        }
        LOG.trace("Existing the joinPropertiesByContext method");
        return propertyAll;
    }

    /**
     * TODO: Place holder for future
     *
     * Load driver.
     *
     * @param step
     *            the step
     * @return the job runner
     */
    protected BpsJobRunner loadDriver(final BpsStep step) {
        try {
            LOG.trace("Entering the loadDriver method");
            final BpsJobRunner jobRunner = (BpsJobRunner) this.getClass().getClassLoader()
                    .loadClass(step.getconfiguration().getProperty("driver-class")).newInstance();
            LOG.trace("Existing the loadDriver method");
            return jobRunner;
        } catch (final Exception e) {
            throw new IllegalStateException("Could not load the driver programme." + e);
        }
    }

    /**
     * This method sets properties.
     *
     * @param propertyAll
     *            the property all
     * @param context
     *            the context
     * @param configuration
     *            the configuration
     */
    private void mapProperties(final Properties propertyAll, final String context, final Properties configuration) {
        LOG.trace("Entering the mapProperties method for context {}", context);
        final Enumeration<?> propertyNames = configuration.propertyNames();
        while (propertyNames.hasMoreElements()) {
            final String name = (String) propertyNames.nextElement();
            propertyAll.setProperty(name, configuration.getProperty(name));
        }
        LOG.trace("Existing the mapProperties method");
    }

    /**
     * This method sets path(flow.xml<Paths>).
     */
    @Override
    public void setFlow(final List<PathType> path) {
        LOG.trace("Entering the setFlow method");
        this.pipe = path;
        taskSeq.add(path.get(0).getFrom().getUri());
        final List<ToType> tos = path.get(0).getTo();
        for (final ToType toType : tos) {
            taskSeq.add(toType.getUri());
        }
        LOG.trace("Existing the setFlow method");
    }

    /**
     * Execute method runs all Step of a pipeline job.
     */
    @Override
    public void execute() {
        LOG.trace("Entering the execute method");
        for (final BpsStep step : steps) {
            step.execute();
        }
        LOG.trace("Existing the execute method");
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the pipe.
     *
     * @return the pipe
     */
    public List<PathType> getPipe() {
        return pipe;
    }

    /**
     * Gets the task seq.
     *
     * @return the taskSeq
     */
    public List<String> getTaskSeq() {
        return taskSeq;
    }

    /**
     * Gets the version.
     *
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Gets the service.
     *
     * @return the service
     */
    public static ServiceLoader<BpsJobRunner> getService() {
        return service;
    }

    /**
     * Sets the service.
     *
     * @param service
     *            the service to set
     */
    public static void setService(final ServiceLoader<BpsJobRunner> service) {
        BpsPipelineRunner.service = service;
    }

    /**
     * Gets the ip adapters.
     *
     * @return the ip adapters
     */
    public BpsDataSourceAdapters getIpAdapters() {
        return ipAdapters;
    }

    /**
     * Gets the op adapters.
     *
     * @return the op adapters
     */
    public BpsDataSinkAdapters getOpAdapters() {
        return opAdapters;
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Gets the path.
     *
     * @return the path
     */
    public List<PathType> getPath() {
        return path;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public void setNAMESPACE(final String nameSpace) {
        this.namespace = nameSpace;

    }

    @Override
    public void setVERSION(final String version) {
        this.version = version;
    }

    /**
     * This operation will do the clean up for Job's Step handlers.
     */
    @Override
    public void cleanUp() {
        LOG.trace("Entering the cleanUp method");
        // clean steps
        for (final BpsStep step : steps) {
            step.cleanUp();
        }
        LOG.trace("Existing the cleanUp method");
    }

    @Override
    public String toString() {
        return "PipelineRunner [ipAdapters=" + ipAdapters + ", opAdapters=" + opAdapters + ", pipe=" + pipe + ", taskSeq=" + taskSeq + ", name="
                + name + ", version=" + version + ", namespace=" + namespace + ", path=" + path + ", steps=" + steps + "]";
    }
}

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
package com.ericsson.component.aia.bps.core.service.streams;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.URIDefinition;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.BpsDataSinkService;
import com.ericsson.component.aia.bps.core.service.partition.BpsPartition;

/**
 * AbstractBpsDataSink provides common bps data sink api implementations
 *
 * @param <C>
 *            Context of the data sink
 * @param <O>
 *            type to write
 *
 */
public abstract class BpsAbstractDataSink<C, O> implements BpsDataSinkService<C, O> {

    private static final Logger LOG = LoggerFactory.getLogger(BpsAbstractDataSink.class);

    /** The writing context. */
    protected String writingContext;

    /** The strategy. */
    protected BpsPartition<?> strategy;

    /** The partition strategy. */
    protected boolean partitionStrategy;

    /** The sink context name. */
    private String dataSinkContextName;

    /** The properties. */
    private Properties properties;

    /** The context. */
    private C context;

    /**
     * Configures the bps data sink.
     *
     * @param dataSinkContextName
     *            the bps data sink context name
     * @param properties
     *            the properties of the bps data sink
     * @param context
     *            the context of execution
     */
    @Override
    public void configureDataSink(final C context, final Properties properties, final String dataSinkContextName) {
        LOG.trace("Entering the DataWriter constructor");
        this.dataSinkContextName = dataSinkContextName;
        this.context = context;
        this.properties = properties;
        final URIDefinition<IOURIS> decode = IOURIS.decode(this.properties.getProperty(Constants.URI));
        this.writingContext = decode.getContext();
        LOG.trace("Existing the DataWriter constructor");
    }

    /**
     * Sets the partition strategy.
     *
     * @param strategy
     *            the new partition strategy
     */
    public void setPartitionStrategy(final BpsPartition<?> strategy) {
        this.strategy = strategy;
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
     *            the properties to set
     */
    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    /**
     * Gets the sink context name.
     *
     * @return the sink context name
     */
    @Override
    public String getDataSinkContextName() {
        return dataSinkContextName;
    }

    /**
     * Gets the writing context.
     *
     * @return the writingContext
     */
    public String getWritingContext() {
        return writingContext;
    }

    /**
     * Gets the context.
     *
     * @return the context
     */
    public C getContext() {
        return context;
    }

    /**
     * Gets the partition strategy.
     *
     * @return the partition strategy
     */
    public BpsPartition<?> getPartitionStrategy() {
        return strategy;
    }

    /**
     * Checks if is partition strategy.
     *
     * @return true, if is partition strategy
     */
    public boolean isPartitionStrategy() {
        return partitionStrategy;

    }

    /**
     * This operation will do the clean up for writers.
     */
    @Override
    public abstract void cleanUp();
}

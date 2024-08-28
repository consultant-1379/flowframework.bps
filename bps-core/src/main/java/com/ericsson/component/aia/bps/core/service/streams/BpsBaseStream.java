package com.ericsson.component.aia.bps.core.service.streams;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>BaseStream</code> is generic data representation of BPS data service configuration(s).
 *
 * Please see the documentation for the details about supported bps data source implementation. Provider specific implementation must supplied by the
 * designer.
 *
 * @param <T>
 *            type of base stream
 */
public class BpsBaseStream<T> implements BpsStream<T> {

    /** Logger */
    private static final Logger LOG = LoggerFactory.getLogger(BpsBaseStream.class);

    /** Current bps data context name from the flow xml input name . */
    protected String contextName;

    /** Current bps data streamRef object of type T */
    protected T streamRef;

    /** Properties of the current bps data configuration from flow xml */
    protected Properties properties;

    /**
     * Instantiates a new base stream.
     *
     * @param context
     *            the context
     * @param streamRef
     *            the stream ref
     */
    public BpsBaseStream(final String context, final T streamRef) {
        LOG.trace("Entering the BaseStream method");
        this.contextName = context;
        this.streamRef = streamRef;
        LOG.trace("Existing the BaseStream method");
    }

    /**
     * Instantiates a new base stream.
     *
     * @param context
     *            the context
     * @param streamRef
     *            the stream ref
     * @param properties
     *            the properties
     */
    public BpsBaseStream(final String context, final T streamRef, final Properties properties) {
        LOG.trace("Calling BaseStream BaseStream");
        this.contextName = context;
        this.streamRef = streamRef;
        this.properties = properties;
        LOG.trace("Existing BaseStream BaseStream");
    }

    /**
     * Instantiates a new base stream.
     */
    public BpsBaseStream() {

    }

    @Override
    public T getStreamRef() {
        return streamRef;
    }

    @Override
    public void setStreamRef(final T streamRef) {
        this.streamRef = streamRef;
    }

    @Override
    public String getContextName() {
        return contextName;
    }

    @Override
    public void setContextName(final String contextName) {
        this.contextName = contextName;
    }

    /**
     * Returns the current input adapter configuration from flow xml
     */
    @Override
    public Properties getProperties() {
        return properties;
    }
}

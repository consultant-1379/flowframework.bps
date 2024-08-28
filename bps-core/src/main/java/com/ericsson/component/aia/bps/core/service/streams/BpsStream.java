package com.ericsson.component.aia.bps.core.service.streams;

import java.util.Properties;

/**
 * <code>Stream</code> interface configures and stores generic stream object. These are used by 3pp frameworks like Spark or flink, etc
 *
 *
 * Provider specific implementation must be supplied by the designer.
 *
 * @param <T>
 *            the generic stream type
 */
public interface BpsStream<T> {

    /**
     * Sets the Input/Output adapter stream name.
     *
     * @param context
     *            Input/Output adapter stream name
     */
    void setContextName(String context);

    /**
     * Sets the Input/Output adapter stream object.
     *
     * @param streamRef
     *            Input/Output adapter stream object
     */
    void setStreamRef(T streamRef);

    /**
     * Returns adapter object of type T as declared in flow xml.
     *
     * @return stream object
     */
    T getStreamRef();

    /**
     * Returns the adapter name as declared in flow xml.
     *
     * In Flow xml, adapter name is represented by tag &lt;input name="input_adapter"&gt; or &lt;output name="output_adapter"&gt;
     *
     * @return the adapter name
     */
    String getContextName();

    /**
     * Returns the adapter configurations as declared in flow xml.
     *
     * @return adapter configuration
     */
    Properties getProperties();
}

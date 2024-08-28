/*
 *
 */
package com.ericsson.component.aia.bps.core.service.streams;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.utils.PropertiesReader;

/**
 * The class for holding or referring Bps data source stream reference object for any context.
 */
public class BpsInputStreams implements Serializable {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesReader.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The data points. */
    private final Map<String, BpsBaseStream<?>> dataPoints = new HashMap<>();

    /**
     * Adds a inputstream to dataPoints.
     *
     * @param dataSourceContext
     *            the context
     * @param streamObj
     *            the stream obj
     */
    public void add(final String dataSourceContext, final BpsBaseStream<?> streamObj) {
        LOG.trace("Entering the add  method ");
        dataPoints.put(dataSourceContext.toLowerCase(), streamObj);
        LOG.trace("Returning the add method ");
    }

    /**
     * Gets the streams.
     *
     * @param <T>
     *            the generic type
     * @param dataSourceContext
     *            the context
     * @return the streams
     */
    @SuppressWarnings("unchecked")
    public <T> BpsStream<T> getStreams(final String dataSourceContext) {
        LOG.trace("Entering the getStreams method ");
        final BpsBaseStream<?> baseStream = dataPoints.get(dataSourceContext.toLowerCase());
        if (null != baseStream) {
            return (BpsStream<T>) baseStream;
        }
        LOG.trace("Returning the getStreams method ");
        throw new IllegalStateException("Could not locate any a streams associated with dataSourceContext specified.");
    }
}

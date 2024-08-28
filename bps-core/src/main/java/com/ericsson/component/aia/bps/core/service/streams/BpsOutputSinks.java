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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.service.BpsDataSinkService;

/**
 * <code>BpsOutputSinks</code> class holds the collection of {@link BpsDataSinkService} which are configured through flow.xml.
 */
public class BpsOutputSinks implements Serializable {

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(BpsOutputSinks.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -1816113675497026292L;

    /** Collection of StreamWriters for Output stream. */
    private final List<BpsDataSinkService<?, ?>> dataSinks = new ArrayList<BpsDataSinkService<?, ?>>();

    /**
     * add method adds a adapter to Bps.
     *
     * @param dataSink
     *            the stream
     */
    public void addBpsDataSinkService(final BpsDataSinkService<?, ?> dataSink) {
        LOG.trace("Adding a StreamWriter " + dataSink.getDataSinkContextName());
        dataSinks.add(dataSink);
        LOG.trace("Successfully add StreamWriter " + dataSink.getDataSinkContextName());
    }

    /**
     * Gets the batch streams.
     *
     * @param sinkContextName
     *            the context
     * @return the batch streams
     */
    public BpsDataSinkService<?, ?> getBpsDataSinkService(final String sinkContextName) {
        LOG.trace("Get a StreamWriter based on the context: " + sinkContextName);
        for (final BpsDataSinkService<?, ?> dataSink : dataSinks) {
            if (dataSink.getDataSinkContextName().equalsIgnoreCase(sinkContextName)) {
                return dataSink;
            }
        }
        throw new IllegalStateException("Could not locate any a Batch streams associated with context specified.");
    }

    /**
     * Write.
     *
     * @param <OUT>
     *            the generic type
     * @param dataStream
     *            the data stream
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <OUT> void write(final OUT dataStream) {
        LOG.trace("started writing data to output stream");
        for (final BpsDataSinkService dataSink : dataSinks) {
            dataSink.write(dataStream);
        }
        LOG.trace("Successfully written data to output stream");
    }

    /**
     * CleanUp method does a cleanup operation for all output streams after writing it to stream.
     */
    public void cleanUp() {
        LOG.trace("cleaning up all output streams");
        for (final BpsDataSinkService<?, ?> dataSink : dataSinks) {
            dataSink.cleanUp();
        }
        LOG.trace("finished cleaning up all output streams");
    }
}

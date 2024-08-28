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
package com.ericsson.component.aia.bps.core.common;

import java.util.Properties;

/**
 * The <code>URIDefinition</code> provides a definition URI. <br>
 * The BPS expected URI like PROTOCOL://CONTEXT?PARAMS <br>
 * Example :
 * <ol>
 * <li>kafka://kafkaTopic?type=avro&bootstrap.servers=host1:9099,host2:9093</li>
 * <li>hive://CELL_HIVE_ANA_PARQ_DEMO</li>
 * </ol>
 *
 * @param <T>
 *            type of Enum to which URIDefinition is associated with
 */
public class URIDefinition<T> {

    String context;
    T protocol;
    Properties properties;

    /**
     * Constructor which takes protocol and Content as argument whereas properties are empty.
     *
     * @param protocol
     *            Protocol name associated with decoded URI.
     * @param context
     *            Content value associated with decoded URI.
     */
    public URIDefinition(final T protocol, final String context) {
        super();
        this.context = context;
        this.protocol = protocol;
        this.properties = new Properties();
    }

    /**
     * Constructor which takes protocol and Content as argument whereas properties are empty.
     *
     * @param protocol
     *            Protocol name associated with decoded URI.
     * @param context
     *            Content value associated with decoded URI.
     * @param properties
     *            list of parameters which was associated with decode URI.
     */
    public URIDefinition(final T protocol, final String context, final Properties properties) {
        super();
        this.context = context;
        this.protocol = protocol;
        this.properties = properties != null ? properties : new Properties();
    }

    /**
     * @return It will return content part of the URI.
     */
    public String getContext() {
        return context;
    }

    /**
     * @return It will return protocol part of the URI.
     */
    public T getProtocol() {
        return protocol;
    }

    /**
     * @return It will return params part of the URI.
     */
    public Properties getParams() {
        return properties;
    }
}

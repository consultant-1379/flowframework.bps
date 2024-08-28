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
package com.ericsson.component.aia.bps.core.common.uri;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.URIDefinition;
import com.google.common.base.Preconditions;

/**
 * The <code>ProcessURIS</code>consists of list of basic Step's URI support by BPS. <br>
 * BPS Supports
 * <ul>
 * <li>SPARK SQL</li>
 * <li>SPARK BATCH</li>
 * <li>SPARK STEARMING</li>
 * </ul>
 *
 */
public enum PROCESS_URIS {

    SPARK_SQL("spark-sql://"),
    /** The spark batch. */
    SPARK_BATCH("spark-batch://"),
    /** The spark batch. */
    SPARK_BATCH_LOCAL("spark-batch-local://"),
    /** The spark streaming. */
    SPARK_STREAMING("spark-streaming://"),
    /** The flink streaming. */
    FLINK_STREAMING("flink-streaming://");

    private static final Pattern pattern = Pattern.compile("^(.+?:/{2,3})(.+\\?*)*(.*)*");

    /** The uri. */
    private String processUri;

    /**
     * Instantiates a new uris.
     *
     * @param uri
     *            Name of the URI.
     */
    PROCESS_URIS(final String uri) {
        this.processUri = uri;
    }

    /**
     * Gets the uri.
     *
     * @return the String value associated with URI.
     */
    public String getUri() {
        return processUri;
    }

    /**
     * Set the URI.
     *
     * @param uri
     *            the new uri
     */
    void setUri(final String uri) {
        this.processUri = uri;
    }

    /**
     *
     * This method will return URIS object equivalent to the provided input uri string.
     *
     * @param uri
     *            String value representing URI.
     * @return will return URIS on success else return null.
     */
    public static PROCESS_URIS fromString(final String uri) {
        if (uri != null) {
            for (final PROCESS_URIS b : PROCESS_URIS.values()) {
                if (uri.equalsIgnoreCase(b.getUri())) {
                    return b;
                }
            }
        }
        throw new IllegalArgumentException("Unknow URI type requested" + uri);
    }

    /**
     * Gets the uris.
     *
     * @param props
     *            the props
     * @return the uris
     */
    public static PROCESS_URIS getURIS(final Properties props) {
        return fromString(getUriType(props));
    }

    /**
     * Decode the string representing URI value. <br>
     *
     * @param uri
     *            Valid URI
     * @return URIDecoder decoded value associated with URI
     */
    @SuppressWarnings("CPD-START")
    public static URIDefinition<PROCESS_URIS> decode(final String uri) {

        final Matcher matcher = pattern.matcher(uri);
        matcher.find();
        final String protocol = matcher.group(1);
        String content = matcher.group(2);
        final String[] split = content.split("\\?");
        final Properties properties = new Properties();
        if (split.length == 2) {
            content = split[0].trim();
            if (content.isEmpty()) {
                throw new IllegalArgumentException("Context Name can not be null");
            }

            final String[] parms = split[1].split("\\&");
            for (final String parm : parms) {
                final String[] keyValuePair = parm.split("\\=");
                if (keyValuePair.length == 2) {
                    properties.put(keyValuePair[0], keyValuePair[1]);
                }
            }
        }

        return new URIDefinition<PROCESS_URIS>(fromString(protocol), content, properties);
    }

    /**
     * Gets the uri type.
     *
     * @param props
     *            the props
     * @return the uri type
     */
    @SuppressWarnings("CPD-END")
    public static String getUriType(final Properties props) {

        final String uri = props.getProperty(Constants.URI);
        Preconditions.checkArgument(uri != null && uri.trim().length() > 0, "Could not locate a valid uri, please check the Flow file.");
        return decode(uri).getProtocol().getUri();
    }

}

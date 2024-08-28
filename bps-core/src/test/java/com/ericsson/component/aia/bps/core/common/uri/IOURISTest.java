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
package com.ericsson.component.aia.bps.core.common.uri;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ericsson.component.aia.bps.core.common.URIDefinition;

/**
 * The <code>IOURISTest</code> Test the functionality of PROCESS_URIS.
 */
public class IOURISTest {
    /**
     * Validate supported URI type.
     */
    @Test
    public void testValidSupportedURI() {
        assertEquals(IOURIS.AMQ, IOURIS.fromString("amq://"));
        assertEquals(IOURIS.DRILL, IOURIS.fromString("drill://"));
        assertEquals(IOURIS.FILE, IOURIS.fromString("file:///"));
        assertEquals(IOURIS.HIVE, IOURIS.fromString("hive://"));
        assertEquals(IOURIS.KAFKA, IOURIS.fromString("kafka://"));
        assertEquals(IOURIS.ALLUXIO, IOURIS.fromString("alluxio://"));
        assertEquals(IOURIS.ZMQ, IOURIS.fromString("zmq://"));
        assertEquals(IOURIS.HDFS, IOURIS.fromString("hdfs://"));
        assertEquals(IOURIS.JDBC, IOURIS.fromString("jdbc://"));
        assertEquals(IOURIS.WEBSOCKET, IOURIS.fromString("web-socket://"));
    }

    /**
     * Test unknown URI type.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedURI() {
        IOURIS.fromString("abc://");
    }

    /**
     * Validate Decode functionality.
     */
    @Test
    public void testDecodeURI() {
        final String kafka = "kafka://ashish?type=avro&filter=*RRC*";
        final URIDefinition<IOURIS> decode = IOURIS.decode(kafka);
        assertEquals(IOURIS.KAFKA, decode.getProtocol());
        assertEquals("ashish", decode.getContext());
        assertEquals("avro", decode.getParams().getProperty("type"));
        assertEquals("*RRC*", decode.getParams().getProperty("filter"));
    }

    /**
     * Validate Jdbc URI for Decode functionality.
     */
    @Test
    public void testJdbcDecodeURI() {
        final String jdbc = "JDBC://jdbc:postgresql://127.0.0.1:5432/saj";
        final URIDefinition<IOURIS> decode = IOURIS.decode(jdbc);
        assertEquals(IOURIS.JDBC, decode.getProtocol());
        assertEquals("jdbc:postgresql://127.0.0.1:5432/saj", decode.getContext());
    }

    /**
     * Validate File URI for Decode functionality.
     */
    @Test
    public void testFileDecodeURI() {
        final String file = "file:////temp/mydoc";
        final URIDefinition<IOURIS> decode = IOURIS.decode(file);
        assertEquals(IOURIS.FILE, decode.getProtocol());
        assertEquals("/temp/mydoc", decode.getContext());
    }
}

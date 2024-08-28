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
 * The <code>PROCESS_URISTest</code> Test the functionality of PROCESS_URIS.
 */
public class PROCESS_URISTest {

    /**
     * Validate supported URI type.
     */
    @Test
    public void testValidSupportedURI() {
        assertEquals(PROCESS_URIS.SPARK_BATCH, PROCESS_URIS.fromString("spark-batch://"));
        assertEquals(PROCESS_URIS.SPARK_STREAMING, PROCESS_URIS.fromString("spark-streaming://"));
        assertEquals(PROCESS_URIS.SPARK_SQL, PROCESS_URIS.fromString("spark-sql://"));
    }

    /**
     * Test unknown URI type.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedURI() {
        PROCESS_URIS.fromString("abc://");
    }

    /**
     * Validate Decode functionality.
     */
    @Test
    public void testDecodeURI() {
        final String sparkBatch = "spark-batch://ctrEvent-analysis";
        final URIDefinition<PROCESS_URIS> decode = PROCESS_URIS.decode(sparkBatch);
        assertEquals(PROCESS_URIS.SPARK_BATCH, decode.getProtocol());
        assertEquals("ctrEvent-analysis", decode.getContext());
    }
}

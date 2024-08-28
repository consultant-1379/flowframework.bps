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

/**
 * The Enum DataFormat.
 */
public enum DataFormat {

    /** The json. */
    JSON("json"),
    /** The csv. */
    CSV("csv"),
    /** The text. */
    TEXT("text"),
    /** The sequencefile. */
    SEQUENCEFILE("SEQUENCEFILE"),
    /** The rcfile. */
    RCFILE("RCFILE"),
    /** The parquet. */
    PARQUET("parquet"),
    /** The avro. */
    AVRO("avro"),
    /** The orc. */
    ORC("ORC"),
    /** The textfile. */
    TEXTFILE("TEXTFILE"),
    /** The jdbc. */
    JDBC("");

    /** Data Format. */
    public String dataFormat;

    /**
     * Instantiates a new data formats.
     *
     * @param dataFormat
     *            the data format
     */
    DataFormat(final String dataFormat) {
        this.dataFormat = dataFormat;
    }

    /**
     * Gets the data format.
     *
     * @return the dataFormat
     */
    public String getDataFormat() {
        return dataFormat;
    }
}

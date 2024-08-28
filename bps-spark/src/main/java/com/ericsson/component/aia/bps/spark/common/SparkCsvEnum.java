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
package com.ericsson.component.aia.bps.spark.common;

/**
 * The SparkCsvEnum holds options for reading & writing CSV files in local or distributed filesystem as Spark DataFrames .
 */
public enum SparkCsvEnum {

    /** The path. */
    PATH("path"),
    /** The header. */
    HEADER("header"),
    /** The delimiter. */
    DELIMITER("delimiter"),
    /** The quote. */
    QUOTE("quote"),
    /** The escape. */
    ESCAPE("escape"),
    /** The parserlib. */
    PARSERLIB("parserLib"),
    /** The mode. */
    MODE("mode"),
    /** The charset. */
    CHARSET("charset"),
    /** The inferschema. */
    INFERSCHEMA("inferSchema"),
    /** The comment. */
    COMMENT("comment"),
    /** The nullvalue. */
    NULLVALUE("nullValue"),
    /** The dateformat. */
    DATEFORMAT("dateFormat"),
    /** The codec. */
    CODEC("codec"),
    /** The quotemode. */
    QUOTEMODE("quoteMode");

    /** The option. */
    public String option;

    /**
     * Instantiates a new spark csv enum.
     *
     * @param option
     *            the option
     */
    SparkCsvEnum(final String option) {
        this.option = option;
    }

    /**
     * Gets the option.
     *
     * @return the option
     */
    public String getOption() {
        return option;
    }

}

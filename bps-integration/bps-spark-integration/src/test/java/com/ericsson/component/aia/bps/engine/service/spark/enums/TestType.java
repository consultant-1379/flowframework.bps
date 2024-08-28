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
package com.ericsson.component.aia.bps.engine.service.spark.enums;

import java.util.EnumSet;

import com.ericsson.component.aia.bps.core.common.DataFormat;
import com.ericsson.component.aia.bps.engine.service.spark.mockContexts.TestMockBaseContext;
import com.ericsson.component.aia.bps.engine.service.spark.mockContexts.TestMockFileContext;
import com.ericsson.component.aia.bps.engine.service.spark.mockContexts.TestMockHdfsContext;
import com.ericsson.component.aia.bps.engine.service.spark.mockContexts.TestMockHiveContext;
import com.ericsson.component.aia.bps.engine.service.spark.mockContexts.TestMockJdbcContext;

/**
 * The Enum TestType.
 */
public enum TestType {

    /** The hive. */
    HIVE(TestMockHiveContext.class, EnumSet.of(DataFormat.TEXTFILE), EnumSet.of(DataFormat.PARQUET)),
    /** The jdbc. */
    JDBC(TestMockJdbcContext.class, EnumSet.noneOf(DataFormat.class), EnumSet.noneOf(DataFormat.class)),
    /** The file. */
    FILE(TestMockFileContext.class, EnumSet.of(DataFormat.CSV, DataFormat.JSON, DataFormat.PARQUET), EnumSet.of(DataFormat.CSV, DataFormat.JSON, DataFormat.PARQUET)),
    /** The hdfs. */
    HDFS(TestMockHdfsContext.class, EnumSet.of(DataFormat.CSV, DataFormat.JSON, DataFormat.PARQUET), EnumSet.of(DataFormat.CSV, DataFormat.JSON, DataFormat.PARQUET));

    /** The ref. */
    public Class<? extends TestMockBaseContext> ref;

    /** The input data formats. */
    private EnumSet<DataFormat> inputDataFormats;

    /** The output data formats. */
    private EnumSet<DataFormat> outputDataFormats;

    /**
     * Instantiates a new test type.
     *
     * @param ref
     *            the ref
     * @param supportedInputFormats
     *            the supported input formats
     * @param supportedOutputFormats
     *            the supported output formats
     */
    TestType(final Class<? extends TestMockBaseContext> ref, final EnumSet<DataFormat> supportedInputFormats,
             final EnumSet<DataFormat> supportedOutputFormats) {
        this.ref = ref;
        this.inputDataFormats = supportedInputFormats;
        this.outputDataFormats = supportedOutputFormats;
    }

    /**
     * Gets the input data formats.
     *
     * @return the input data formats
     */
    public EnumSet<DataFormat> getInputDataFormats() {
        return inputDataFormats;
    }

    /**
     * Gets the output data formats.
     *
     * @return the output data formats
     */
    public EnumSet<DataFormat> getOutputDataFormats() {
        return outputDataFormats;
    }
}

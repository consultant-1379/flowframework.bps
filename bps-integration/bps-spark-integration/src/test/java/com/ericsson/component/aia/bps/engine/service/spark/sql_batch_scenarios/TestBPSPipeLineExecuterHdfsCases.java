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
package com.ericsson.component.aia.bps.engine.service.spark.sql_batch_scenarios;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.ericsson.component.aia.bps.engine.service.spark.enums.TestType;
import com.ericsson.component.aia.bps.engine.service.spark.mockContexts.TestMockBaseContext;

/**
 * The Class TestBPSPipeLineExecuterHdfsCases.
 */
@RunWith(Parameterized.class)
public class TestBPSPipeLineExecuterHdfsCases extends TestBPSPipeLineExecuterBase {

    /**
     * Instantiates a new test BPS pipe line executer hdfs cases.
     *
     * @param name
     *            the name
     * @param inputType
     *            the input type
     * @param outputType
     *            the output type
     * @param inputDataFormat
     *            the input data format
     * @param outputDataFormat
     *            the output data format
     */
    public TestBPSPipeLineExecuterHdfsCases(final String name, final Class<? extends TestMockBaseContext> inputType,
                                            final Class<? extends TestMockBaseContext> outputType, final String inputDataFormat,
                                            final String outputDataFormat) {
        super(name, inputType, outputType, inputDataFormat, outputDataFormat);
    }

    /**
     * Generates Scenarios matrix set.
     *
     * @return the collection
     * @throws Exception
     *             the exception
     */
    @Parameters(name = "{index}: Validating Scenario [ {0} ]")

    public static Collection<Object[]> data() throws Exception {
        return provideData(TestType.HDFS);
    }
}

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
package com.ericsson.component.aia.bps.spark.jobrunner.common;

import static com.ericsson.component.aia.bps.spark.jobrunner.common.SparkAppFooter.closeSparkContext;
import static com.ericsson.component.aia.bps.spark.jobrunner.common.SparkAppFooter.saveData;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

public class SparkAppFooterTest {

    @Test
    public void saveDataShouldCallSaveAsTextFile() {
        JavaRDD<?> record = mock(JavaRDD.class);
        String path = "path";
        saveData(path, record);
        verify(record, times(1)).saveAsTextFile(path);
    }

    @Test
    public void closeSparkContextShouldCallClose() {
        JavaSparkContext jsc = mock(JavaSparkContext.class);
        closeSparkContext(jsc);
        verify(jsc, times(1)).close();
    }

    @Test
    public void closeSparkStreamingContextShouldCallCloase() {
        JavaStreamingContext jsc = mock(JavaStreamingContext.class);
        SparkAppFooter.closeSparkStreamContext(jsc);
        verify(jsc, times(1)).close();
    }

}

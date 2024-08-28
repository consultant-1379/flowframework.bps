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
package com.ericsson.component.aia.bps.spark.configuration.partition;

import static com.ericsson.component.aia.bps.spark.configuration.partition.SparkDefaultPartition.getUnit;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.spark.sql.SaveMode.Append;
import static org.apache.spark.sql.SaveMode.ErrorIfExists;
import static org.apache.spark.sql.SaveMode.Ignore;
import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

public class SparkDefaultPartitionTest {

    SparkDefaultPartition sdp = new SparkDefaultPartition();
    Properties props = mock(Properties.class);

    @Test
    public void getUnitShouldReturnCorrectTimeUnit() {

        assertEquals(DAYS, getUnit("days"));
        assertEquals(HOURS, getUnit("hh"));
        assertEquals(MINUTES, getUnit("mm"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getUnitShouldThrowUnsupportedOperationExceptionForInvalidInput() {

        getUnit("ms");
    }

    @Test
    public void getUserDefinedPartitionsShouldReturnColumnsFromProperties() {

        when(props.getProperty("partition.columns")).thenReturn("a,b");
        String[] expected = { "a", "b" };
        assertArrayEquals(expected, sdp.getUserDefinedPartitions(props));
    }

    @Test
    public void initShouldSetPrivateVariablesUsingProperties()
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

        when(props.getProperty("partition.columns")).thenReturn("a,b");
        when(props.getProperty("data.format")).thenReturn("text");
        when(props.getProperty("data.save.mode", "Append")).thenReturn("Overwrite");
        sdp.init(props);

        boolean isPartitionEnabledValue = (Boolean) accesssPrivateVariable("isPartitionEnabled").get(sdp);
        String[] pColumnsValue = (String[]) accesssPrivateVariable("pColumns").get(sdp);
        String dFormatValue = (String) accesssPrivateVariable("dFormat").get(sdp);
        boolean isTextFormatValue = (Boolean) accesssPrivateVariable("isTextFormat").get(sdp);
        SaveMode writeModeValue = (SaveMode) accesssPrivateVariable("writeMode").get(sdp);

        String[] expectedPColumnsValue = { "a", "b" };

        assertEquals(true, isPartitionEnabledValue);
        assertArrayEquals(expectedPColumnsValue, pColumnsValue);
        assertEquals("text", dFormatValue);
        assertEquals(true, isTextFormatValue);
        assertEquals(Append, writeModeValue);

    }

    @Test
    public void initShouldSetPrivateVariablesUsingNullProperties()
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

        when(props.getProperty("partition.columns")).thenReturn(null);
        when(props.getProperty("data.format")).thenReturn(null);
        when(props.getProperty("data.save.mode", "Append")).thenReturn(null);
        sdp.init(props);

        boolean isPartitionEnabledValue = (Boolean) accesssPrivateVariable("isPartitionEnabled").get(sdp);
        String[] pColumnsValue = (String[]) accesssPrivateVariable("pColumns").get(sdp);
        String dFormatValue = (String) accesssPrivateVariable("dFormat").get(sdp);
        boolean isTextFormatValue = (Boolean) accesssPrivateVariable("isTextFormat").get(sdp);
        SaveMode writeModeValue = (SaveMode) accesssPrivateVariable("writeMode").get(sdp);

        assertEquals(false, isPartitionEnabledValue);
        assertArrayEquals(null, pColumnsValue);
        assertEquals("parquet", dFormatValue);
        assertEquals(false, isTextFormatValue);
        assertEquals(Append, writeModeValue);
    }

    @Test
    public void initShouldSetWriteModeBasedOnDataFormatProperty()
            throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {

        when(props.getProperty("data.save.mode", "Append")).thenReturn("saveMode");
        when(props.getProperty("data.format")).thenReturn("Overwrite");
        sdp.init(props);
        SaveMode writeModeValue = (SaveMode) accesssPrivateVariable("writeMode").get(sdp);
        assertEquals(Overwrite, writeModeValue);

        when(props.getProperty("data.format")).thenReturn("Append");
        sdp.init(props);
        SaveMode writeModeValue1 = (SaveMode) accesssPrivateVariable("writeMode").get(sdp);
        assertEquals(Append, writeModeValue1);

        when(props.getProperty("data.format")).thenReturn("ErrorIfExists");
        sdp.init(props);
        SaveMode writeModeValue2 = (SaveMode) accesssPrivateVariable("writeMode").get(sdp);
        assertEquals(ErrorIfExists, writeModeValue2);

        when(props.getProperty("data.format")).thenReturn("Ignore");
        sdp.init(props);
        SaveMode writeModeValue3 = (SaveMode) accesssPrivateVariable("writeMode").get(sdp);
        assertEquals(Ignore, writeModeValue3);
    }

    @Test
    public void writeShouldCallSaveAsTextFile() {
        JavaRDD<?> record = mock(JavaRDD.class);
        String path = "path";
        sdp.write(record, path);
        verify(record, times(1)).saveAsTextFile(path);
    }

    private Field accesssPrivateVariable(String variable) throws NoSuchFieldException, SecurityException {
        Field privateVariable = SparkDefaultPartition.class.getDeclaredField(variable);
        privateVariable.setAccessible(true);
        return privateVariable;
    }

}

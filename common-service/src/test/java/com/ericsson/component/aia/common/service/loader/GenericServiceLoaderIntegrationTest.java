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
package com.ericsson.component.aia.common.service.loader;

import static com.ericsson.component.aia.common.service.common.TestConstants.FILE;
import static com.ericsson.component.aia.common.service.common.TestConstants.HDFS;
import static com.ericsson.component.aia.common.service.common.TestConstants.HIVE;
import static com.ericsson.component.aia.common.service.common.TestConstants.JDBC;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ServiceConfigurationError;

import org.junit.Test;

import com.ericsson.component.aia.common.service.GenericService;
import com.ericsson.component.aia.common.service.exception.ServiceNotFoundException;
import com.ericsson.component.aia.common.service.loader.spi.InputStreamService;
import com.ericsson.component.aia.common.service.loader.spi.OutputStreamService;
import com.ericsson.component.aia.common.service.loader.spi.impl.FileInputStreamService;
import com.ericsson.component.aia.common.service.loader.spi.impl.FileOutputStreamService;
import com.ericsson.component.aia.common.service.loader.spi.impl.HdfsInputStreamService;
import com.ericsson.component.aia.common.service.loader.spi.impl.HdfsOutputStreamService;

/**
 * Junit class to test GenericServiceLoader
 *
 * @author echchik
 *
 */
public class GenericServiceLoaderIntegrationTest {

    @Test
    public void shouldReturnFileInputStreamServiceInstanceWhenLoaderIsCalledWithInputStreamServiceAndFileArgument() {
        final GenericService genericService = GenericServiceLoader.getService(InputStreamService.class, FILE);
        assertNotNull(genericService);
        assertTrue(genericService instanceof FileInputStreamService);
    }

    @Test
    public void shouldReturnHdfsInputStreamServiceInstanceWhenLoaderIsCalledWithInputStreamServiceAndHdfsArgument() {
        final GenericService genericService = GenericServiceLoader.getService(InputStreamService.class, HDFS);
        assertNotNull(genericService);
        assertTrue(genericService instanceof HdfsInputStreamService);
    }

    @Test
    public void shouldReturnFileOutputStreamServiceInstanceWhenLoaderIsCalledWithOutputStreamServiceAndFileArgument() {
        final GenericService genericService = GenericServiceLoader.getService(OutputStreamService.class, FILE);
        assertNotNull(genericService);
        assertTrue(genericService instanceof FileOutputStreamService);
    }

    @Test
    public void shouldReturnHdfsOutputStreamServiceInstanceWhenLoaderIsCalledWithOutputStreamServiceAndHdfsArgument() {
        final GenericService genericService = GenericServiceLoader.getService(OutputStreamService.class, HDFS);
        assertNotNull(genericService);
        assertTrue(genericService instanceof HdfsOutputStreamService);

    }

    @Test(expected = ServiceNotFoundException.class)
    public void shouldThrowServiceNotFoundExceptionWhenServiceIsNotPresent() {
        GenericServiceLoader.getService(OutputStreamService.class, HIVE);
    }

    @Test(expected = ServiceConfigurationError.class)
    public void shouldThrowRuntimeExceptionWhenServiceImplHasNoArgumentConstructor() {
        GenericServiceLoader.getService(InputStreamService.class, JDBC);
    }
}

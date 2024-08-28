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
package com.ericsson.component.aia.common.service.loader.spi.impl;

import static com.ericsson.component.aia.common.service.common.TestConstants.HDFS;

import com.ericsson.component.aia.common.service.loader.spi.InputStreamService;

/**
 * HDFS InputStreamService implementation
 *
 * @author echchik
 *
 */
public class HdfsInputStreamService implements InputStreamService {

    @Override
    public String getServiceContextName() {
        return HDFS;
    }

}

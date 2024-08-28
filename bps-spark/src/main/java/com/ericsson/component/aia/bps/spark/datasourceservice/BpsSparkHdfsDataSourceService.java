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
package com.ericsson.component.aia.bps.spark.datasourceservice;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.ericsson.component.aia.bps.spark.utils.SparkUtil;

/**
 * The <code>BpsSparkHdfsDataSourceService</code> is responsible for reading data from hdfs system and return respective {@link DataFrame } .<br>
 *
 * The <code>BpsSparkHdfsDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, DataFrame&gt;</code> which is specific to
 * HiveContext & DataFrame. <br>
 * <br>
 */
public class BpsSparkHdfsDataSourceService extends AbstractBpsDataSourceService<HiveContext, DataFrame> {

    /**
     * The LOGGER. private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkHdfsDataSourceService.class);
     */

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.HDFS.getUri();
    }

    /**
     * Gets the DataFrame based on the provided configurations in flow xml.
     *
     * @return the data stream
     */
    @Override
    public DataFrame getDataStream() {
        return SparkUtil.getDataFrame(context, properties);
    }
}

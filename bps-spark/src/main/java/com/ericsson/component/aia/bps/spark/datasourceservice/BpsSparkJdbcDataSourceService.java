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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.URIDefinition;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.google.common.base.Preconditions;

/**
 * The <code>BpsSparkJdbcDataSourceService</code> is responsible for reading data from jdbc system and return respective {@link DataFrame } .<br>
 *
 * The <code>BpsSparkJdbcDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, DataFrame&gt;</code> which is specific to
 * HiveContext & DataFrame. <br>
 * <br>
 */
public class BpsSparkJdbcDataSourceService extends AbstractBpsDataSourceService<HiveContext, DataFrame> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkJdbcDataSourceService.class);

    /**
     * Gets the service context name as defined in flow xml.
     *
     * @return the service context name
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.JDBC.getUri();
    }

    /**
     * Gets the DataFrame based on the provided configurations in flow xml.
     *
     * @return the data stream
     */
    @Override
    public DataFrame getDataStream() {
        LOGGER.trace("Entering the getJDBCTableContexts method ");
        Preconditions.checkState(context != null, "Spark context is not initialized.");
        final String uri = properties.getProperty(Constants.URI);

        Preconditions.checkArgument(uri != null, "Invalid URI ");
        final URIDefinition<IOURIS> decode = IOURIS.decode(uri);
        final String tableName = properties.getProperty(Constants.TABLE);
        Preconditions.checkArgument(tableName != null, "Invalid table name ");
        Preconditions.checkArgument(tableName.trim().length() > 0, "Invalid table name ");
        final DataFrame jdbc = context.read().jdbc(decode.getContext(), tableName, properties);
        jdbc.registerTempTable(tableName);
        LOGGER.trace("Returning the getJDBCTableContexts method ");
        return jdbc;
    }

}

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
package com.ericsson.component.aia.bps.spark.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCUtilities is a utility class for all JDBC related operations.
 */
public class JDBCUtilities {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(JDBCUtilities.class);

    private JDBCUtilities() {

    }

    /**
     * Clean target.
     *
     * @param url
     *            the url
     * @param connectionProperties
     *            the connection properties
     * @param tableName2
     *            the table name 2
     */
    public static void cleanTarget(final String url, final Properties connectionProperties, final String tableName2) {
        LOG.trace("Entering the cleanTarget method ");
        Connection createConnection = null;
        try {
            createConnection = create(url, connectionProperties);
            final boolean tableExists = JdbcUtils.tableExists(createConnection, url, tableName2);
            if (tableExists) {
                JdbcUtils.dropTable(createConnection, tableName2);
            }
        } finally {
            if (createConnection != null) {
                try {
                    createConnection.close();
                } catch (final SQLException exp) {
                    LOG.error("Exception occurred while closing the connection, reason={}", exp.getMessage());
                }
            }
        }

        LOG.trace("Existing from the cleanTarget method ");
    }

    /**
     * Creates the.
     *
     * @param url
     *            the url
     * @param connectionProperties
     *            the connection properties
     * @return the connection
     */
    public static Connection create(final String url, final Properties connectionProperties) {
        return JdbcUtils.createConnection(url, connectionProperties);
    }

    /**
     * Exist.
     *
     * @param conn
     *            the conn
     * @param url
     *            the url
     * @param table
     *            the table
     * @return true, if successful
     */
    public static boolean exist(final Connection conn, final String url, final String table) {
        return JdbcUtils.tableExists(conn, url, table);
    }

    /**
     * Gets the JDBC properties.
     *
     * @param config
     *            the getconfiguration
     * @return the JDBC properties
     */
    public static Properties getJDBCProperties(final Properties config) {
        LOG.trace("Entering the getJDBCProperties method ");
        final Properties props = new Properties();

        for (final Object entry : config.keySet()) {

            final String key = (String) entry;
            if (key.toLowerCase().startsWith("jdbc")) {

                props.setProperty(key.replace("jdbc.", ""), config.getProperty(key));
            }
        }
        LOG.trace("Existing from the getJDBCProperties method ");
        return props;
    }
}

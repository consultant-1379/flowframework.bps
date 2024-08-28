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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.DataFormat;
import com.ericsson.component.aia.bps.spark.common.SparkCsvEnum;
import com.google.common.base.Preconditions;

/**
 * SparkUtil is a utility class for all Spark related operations.
 */
public class SparkUtil {

    private SparkUtil() {

    }

    /**
     * Creates a dataframe object based on the passed data frame.
     *
     * @param context
     *            the context
     * @param properties
     *            the properties
     * @return the data frame
     */
    public static DataFrame getDataFrame(final HiveContext context, final Properties properties) {

        final String tableName = properties.getProperty("table-name");
        final String format = properties.getProperty(Constants.DATA_FORMAT);
        final String file = properties.getProperty(Constants.URI);

        Preconditions.checkState(contains(format), "invalid data format.");

        if (DataFormat.CSV.dataFormat.equals(format)) {

            final Map<String, String> options = getOptions(properties);
            final DataFrame frame = context.read().format("com.databricks.spark.csv").options(options).load(file);
            frame.registerTempTable(tableName);
            return frame;
        } else {

            final DataFrame frame = context.read().format(format).load(file);
            frame.registerTempTable(tableName);
            return frame;
        }
    }

    /**
     * Gets the options.
     *
     * @param properties
     *            the properties
     * @return the options
     */
    public static Map<String, String> getOptions(final Properties properties) {

        final Map<String, String> options = new HashMap<>();

        for (final SparkCsvEnum sparkCsv : SparkCsvEnum.values()) {

            if (properties.containsKey(sparkCsv.getOption())) {
                options.put(sparkCsv.getOption(), properties.getProperty(sparkCsv.getOption()));
            }

        }
        return options;
    }

    private static boolean contains(final String test) {

        for (final DataFormat type : DataFormat.values()) {
            if (type.dataFormat.equals(test)) {
                return true;
            }
        }

        return false;
    }
}

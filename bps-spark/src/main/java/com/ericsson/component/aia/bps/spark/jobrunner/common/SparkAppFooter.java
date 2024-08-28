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

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has common code for the spark contexts.
 */
public class SparkAppFooter implements Serializable {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(SparkAppFooter.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 5643977810781373197L;

    private SparkAppFooter() {

    }

    /**
     * This method is responsible to Save data as text file.
     *
     * @param <V>
     *            the value type
     * @param path
     *            the path
     * @param record
     *            the record
     */
    public static <V> void saveData(final String path, final JavaRDD<V> record) {
        LOG.trace("Entering the saveData method");
        record.saveAsTextFile(path);
        LOG.trace("Existing the saveData method");
    }

    /**
     * This method is responsible to close {@link JavaSparkContext} provided as argument
     *
     * @param ctx
     *            {@link JavaSparkContext} provided as argument to close
     */
    public static void closeSparkContext(final JavaSparkContext ctx) {

        if (null != ctx) {
            LOG.trace("Closing JavaSparkContext");
            ctx.close();
            LOG.trace("Successfully closed JavaSparkContext");
        }
    }

    /**
     * This method is responsible to close {@link JavaStreamingContext} provided as argument
     *
     * @param ctx
     *            {@link JavaStreamingContext} provided as argument to close
     */
    public static void closeSparkStreamContext(final JavaStreamingContext ctx) {

        if (null != ctx) {
            LOG.trace("Closing JavaSparkContext");
            ctx.close();
            LOG.trace("Successfully closed JavaStreamingContext");
        }
    }
}

package com.ericsson.component.aia.bps.spark.jobrunner;

import com.ericsson.component.aia.bps.core.common.uri.PROCESS_URIS;

/**
 * SparkBatchJobRunner class is a one of the implementation for Step interface. This handler is used when the user wants to use Spark batch (With Hive
 * Context).
 */
public class SparkBatchJobRunner extends SparkSQLJobRunner {

    private static final long serialVersionUID = -250957532078855504L;

    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_BATCH.getUri();
    }
}

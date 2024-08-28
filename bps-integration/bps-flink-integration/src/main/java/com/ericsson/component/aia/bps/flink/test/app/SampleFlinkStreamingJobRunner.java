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
package com.ericsson.component.aia.bps.flink.test.app;

import java.util.HashMap;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.service.streams.BpsStream;
import com.ericsson.component.aia.bps.flink.jobrunner.BpsFlinkStreamingJobRunner;

/**
 * This class extends {@link BpsFlinkStreamingJobRunner} and provides logic for reading {@link DataStream} from data sources and applying
 * transformation and writing the result to configured data sinks
 */
public class SampleFlinkStreamingJobRunner extends BpsFlinkStreamingJobRunner {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleFlinkStreamingJobRunner.class);

    @Override
    public void executeJob() {
        final BpsStream<DataStream<GenericRecord>> flinkStream = getBpsInputStreams().<DataStream<GenericRecord>> getStreams("input-stream");
        final DataStream<GenericRecord> dataStream = flinkStream.getStreamRef();

        //Some transformation
        final DataStream<HashMap<String, Object>> toHashMap = dataStream.map(new MapFunction<GenericRecord, HashMap<String, Object>>() {
            private static final long serialVersionUID = 3961882164579010828L;

            @Override
            @SuppressWarnings("PMD.SignatureDeclareThrowsException")
            public HashMap<String, Object> map(final GenericRecord genericRecord) throws Exception {
                final HashMap<String, Object> attributes = new HashMap<String, Object>();
                for (final Field field : genericRecord.getSchema().getFields()) {
                    Object col = genericRecord.get(field.name());
                    if (col != null && col instanceof Utf8) {
                        col = col.toString();
                    }
                    attributes.put(field.name(), col);
                }
                return attributes;
            }
        });

        dataStream.print();
        persistDataStream(toHashMap);
        LOGGER.info("EpsFlinkStreamingHandler executeJob successfully completed");

    }

    @Override
    public void cleanUp() {

    }
}

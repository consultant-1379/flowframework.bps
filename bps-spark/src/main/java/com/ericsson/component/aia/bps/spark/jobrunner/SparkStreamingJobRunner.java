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
package com.ericsson.component.aia.bps.spark.jobrunner;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.URIDefinition;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.common.uri.PROCESS_URIS;
import com.ericsson.component.aia.bps.core.service.BpsJobRunner;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasink.BpsDataSinkConfiguration;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceAdapters;
import com.ericsson.component.aia.bps.core.service.configuration.datasource.BpsDataSourceConfiguration;
import com.ericsson.component.aia.bps.core.service.streams.BpsBaseStream;
import com.ericsson.component.aia.bps.core.service.streams.BpsInputStreams;
import com.ericsson.component.aia.bps.spark.configuration.KafkaStreamConfiguration;
import com.ericsson.component.aia.bps.spark.jobrunner.common.SparkAppFooter;
import com.ericsson.component.aia.bps.spark.jobrunner.common.SparkAppHeader;
import com.ericsson.component.aia.bps.spark.utils.DateUtils;

import parquet.avro.AvroParquetOutputFormat;
import parquet.hadoop.ParquetInputFormat;
import scala.Tuple2;

/**
 * SparkStreamingHandler class is a base class and one of the implementation for Step interface. This handler is used when the user wants to run
 * Streaming application using Spark DStream.
 */
public abstract class SparkStreamingJobRunner implements BpsJobRunner {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingJobRunner.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 655833713003724650L;

    /** The jssc. */
    private JavaStreamingContext jssc;

    /** The cal. */
    private final Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    /** The streams. */
    private transient BpsInputStreams streams;

    /** The output paths. */
    private final List<String> outputPaths = new ArrayList<>();

    /**
     * Creates the kafka connection.
     *
     * @param <X>
     *            the generic type
     * @param <Y>
     *            the generic type
     * @param props
     *            the props
     * @return the java pair input D stream
     */
    @SuppressWarnings("unchecked")
    protected <X, Y> JavaPairInputDStream<X, Y> createKafkaConnection(final Properties props) {
        LOG.trace("Entering the createKafkaConnection method");
        return (JavaPairInputDStream<X, Y>) SparkAppHeader.createConnection(new KafkaStreamConfiguration(props));
    }

    /**
     * Execute method runs Step of a pipeline job.
     */
    @Override
    public void execute() {
        LOG.trace("Entering the execute method");
        executeJob();
        getJavaStreamigContext().start();
        getJavaStreamigContext().awaitTermination();
        LOG.trace("Existing the execute method");
    }

    /**
     * executeJob method should be implemented by the respective streaming driver class.
     */
    public abstract void executeJob();

    /**
     * Initializes step handler parameters.
     */
    @Override
    public void initialize(final BpsDataSourceAdapters inputAdapters, final BpsDataSinkAdapters outputAdapters, final Properties properties) {

        LOG.trace("Entering the initialize method");

        if (jssc == null) {
            jssc = SparkAppHeader.init(inputAdapters, outputAdapters, properties);
        }

        streams = new BpsInputStreams();

        for (final BpsDataSourceConfiguration in : inputAdapters.getBpsDataSourceConfigurations()) {

            final Properties config = in.getDataSourceConfiguration();
            final String property = config.getProperty(Constants.URI);
            final URIDefinition<IOURIS> decode = IOURIS.decode(property);

            if (decode.getProtocol() == IOURIS.KAFKA) {
                //final String topics = property.trim().substring(property.lastIndexOf("kafka://") + 1, property.length());
                //config.put(Constants.TOPIC, topics);
                config.put(Constants.TOPIC, decode.getContext());
                streams.add(in.getDataSourceContextName(), new BpsBaseStream<>(in.getDataSourceContextName(), createKafkaConnection(config)));
            }
        }

        understantOutputs(outputAdapters);
        LOG.trace("Existing the initialize method");
    }

    /**
     * Persist RDD as text file based on user provided output streams.
     *
     * @param record
     *            the record
     */
    public void persistRDD(final JavaRDD<?> record) {
        LOG.trace("Entering the persistRDD method");
        if (record.isEmpty()) {
            return;
        }
        final String part = cal.get(Calendar.YEAR) + SEPARATOR + cal.get(Calendar.MONTH) + SEPARATOR + cal.get(Calendar.DATE) + SEPARATOR
                + cal.get(Calendar.HOUR) + SEPARATOR + cal.get(Calendar.MINUTE) + SEPARATOR;
        for (final String path : outputPaths) {
            record.saveAsTextFile(path + SEPARATOR + part + SEPARATOR);
        }
        LOG.trace("Existing the persistRDD method");
    }

    /**
     * Persist RDD as single text file partition based on user provided output streams.
     *
     * @param record
     *            the record
     * @param asSingleRecrod
     *            the as single record
     */
    public void persistRDDSinglePartition(final JavaRDD<?> record, final boolean asSingleRecrod) {
        LOG.trace("Entering the persistRDDSinglePartition method");
        if (record.isEmpty()) {
            return;
        }
        final String part = cal.get(Calendar.YEAR) + SEPARATOR + cal.get(Calendar.MONTH) + SEPARATOR + cal.get(Calendar.DATE) + SEPARATOR
                + cal.get(Calendar.HOUR) + SEPARATOR + cal.get(Calendar.MINUTE) + SEPARATOR;

        for (final String path : outputPaths) {
            LOG.debug(path + SEPARATOR + part + SEPARATOR);
            record.coalesce(1).saveAsTextFile(path + SEPARATOR + part + SEPARATOR);
        }
        LOG.trace("Existing the persistRDDSinglePartition method");
    }

    /**
     * Understant outputs.
     *
     * @param bpsOut
     *            the bps out
     */
    private void understantOutputs(final BpsDataSinkAdapters bpsOut) {
        LOG.trace("Entering the understantOutputs method");
        for (final BpsDataSinkConfiguration out : bpsOut.getBpsDataSinkConfigurations()) {
            final Properties config = out.getDataSinkConfiguration();
            final String uri = config.getProperty(Constants.URI);
            final String string = uri.trim().substring(0, uri.lastIndexOf("/") + 1);
            final URIDefinition<IOURIS> decode = IOURIS.decode(uri);
            switch (decode.getProtocol()) {
                case ALLUXIO:
                    final String master = config.getProperty("master-url");
                    final String path = master + SEPARATOR + string + SEPARATOR;
                    outputPaths.add(path);
                    break;
                case HDFS:
                    final String hdfs = config.getProperty("master-url");
                    final String hdfsPath = hdfs + SEPARATOR + string + SEPARATOR;
                    outputPaths.add(hdfsPath);
                    break;
                case FILE:
                    final String fileRoot = config.getProperty("file.storage.root");
                    outputPaths.add(fileRoot + SEPARATOR + string + SEPARATOR);
                    break;
                default:
                    new IllegalArgumentException(String.format("Could not able to find %s URI", decode.getProtocol().getUri()));
                    break;
            }
        }
        LOG.trace("Existing the understantOutputs method");
    }

    // TODO: implementation needs to finish
    /**
     * Read parquet from user provided path.
     *
     * @param inputPath
     *            the input path
     * @param fromDate
     *            the from date
     * @param toDate
     *            the to date
     * @param format
     *            the format
     * @param sparkContext
     *            the sc
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    public void readParquet(String inputPath, final String fromDate, final String toDate, final String format, final JavaSparkContext sparkContext)
            throws IOException {
        LOG.trace("Entering the readParquet method");
        @SuppressWarnings("deprecation")
        final Job job = new Job();
        final ParquetInputFormat<GenericRecord> inputFormat = new ParquetInputFormat<>();
        ParquetInputFormat.setReadSupportClass(job, GenericRecord.class);

        inputPath = DateUtils.getDates(fromDate, toDate, format);

        @SuppressWarnings("unchecked")
        final JavaPairRDD<Void, GenericRecord> returnObj = sparkContext.newAPIHadoopFile(inputPath, inputFormat.getClass(), Void.class,
                GenericRecord.class, job.getConfiguration());

        sparkContext.textFile("/my/dir1,/my/paths/part-00[0-5]*,/another/dir,/a/specific/file");

        returnObj.filter(new Function<Tuple2<Void, GenericRecord>, Boolean>() {
            private static final long serialVersionUID = 8067933998352876783L;

            @Override
            public Boolean call(final Tuple2<Void, GenericRecord> tuple2) throws Exception {
                return null;
            }
        });
        LOG.trace("Existing the readParquet method");
    }

    /**
     * Persist parquet file based on user provided output based generic record.
     *
     * @param record
     *            the record
     * @param pojoClass
     *            the pojo class
     * @param schema
     *            the schema
     * @param outputPath
     *            the output path
     * @param sparkContext
     *            the sc
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("unchecked")
    public void persistParquet(final JavaRDD<?> record, final Class<GenericRecord> pojoClass, final Schema schema, final String outputPath,
                               final JavaSparkContext sparkContext)
            throws IOException {
        LOG.trace("Entering the persistParquet method");
        @SuppressWarnings("deprecation")
        final Job job = new Job();
        AvroParquetOutputFormat.setSchema(job, schema);

        // Configure the ParquetOutputFormat to use Avro as the serialization
        // format
        // ParquetOutputFormat.setWriteSupportClass(job,
        // classOf[AvroWriteSupport]);

        final ParquetInputFormat<GenericRecord> inputFormat = new ParquetInputFormat<>();
        sparkContext.newAPIHadoopRDD(job.getConfiguration(), inputFormat.getClass(), Void.class, GenericRecord.class);
        LOG.trace("Existing the persistParquet method");
    }

    /**
     * Gets the kafka stream.
     *
     * @param <T>
     *            the generic type
     * @param <V>
     *            the value type
     * @return the kafka stream
     */
    @SuppressWarnings("unchecked")
    protected <T, V> JavaPairInputDStream<T, V> getKafkaStream() {
        return (JavaPairInputDStream<T, V>) streams.getStreams("radio-stream");
    }

    /**
     * Gets the java streamig context.
     *
     * @return the java streamig context
     */
    public JavaStreamingContext getJavaStreamigContext() {
        return jssc;
    }

    @Override
    public String toString() {
        return "SparkStreamingHandler [outputPaths=" + outputPaths + "]";
    }

    @Override
    public String getServiceContextName() {
        return PROCESS_URIS.SPARK_STREAMING.getUri();
    }

    /**
     * @return the streams
     */
    public BpsInputStreams getStreams() {
        return streams;
    }

    @Override
    public void cleanUp() {
        LOG.trace("Entering the cleanUp method");
        SparkAppFooter.closeSparkStreamContext(jssc);
        jssc = null;
        LOG.trace("Existing the cleanUp method");
    }
}
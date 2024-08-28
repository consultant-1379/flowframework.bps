/**
 *
 * (C) Copyright LM Ericsson System Expertise AT/LMI, 2016
 *
 * The copyright to the computer program(s) herein is the property of Ericsson  System Expertise EEI, Sweden.
 * The program(s) may be used and/or copied only with the written permission from Ericsson System Expertise
 * AT/LMI or in  * accordance with the terms and conditions stipulated in the agreement/contract under which
 * the program(s) have been supplied.
 *
 */
package com.ericsson.component.aia.bps.core.common;

import java.io.File;

/**
 * Various constants used across Bps libraries.
 */
public interface Constants {

    /** The spark application. */
    boolean SPARK_APPLICATION = true;

    /** The drill application. */
    boolean DRILL_APPLICATION = false;

    /** The sql. */
    String SQL = "sql";

    /** The schema. */
    String SCHEMA = "schema";

    /** The table. */
    String TABLE = "table.name";

    /** The input. */
    String INPUT = "input";

    /** The spark sql context. */
    String SPARK_SQL_CONTEXT = "sqlContext";

    /** The output. */
    String OUTPUT = "output";

    /** The handler. */
    String HANDLER = "handler";

    /** The name. */
    String NAME = "name";

    /** The namespace. */
    String NAMESPACE = "namespace";

    /** The version. */
    String VERSION = "version";

    /** The uri. */
    String URI = "uri";

    /** The kafka uri. */
    String KAFKA_URI = "kafka://";

    /** The slide win length. */
    String SLIDE_WIN_LENGTH = "slide.win.length";

    /** The win length. */
    String WIN_LENGTH = "win.length";

    /** The kafka value decoder class. */
    String KAFKA_VALUE_DECODER_CLASS = "kafka.valueDecoder.class";

    /** The kafka key decoder class. */
    String KAFKA_KEY_DECODER_CLASS = "kafka.keyDecoder.class";

    /** The kafka value class. */
    String KAFKA_VALUE_CLASS = "kafka.valueClass";

    /** The kafka key class. */
    String KAFKA_KEY_CLASS = "kafka.keyClass";

    /** The topic. */
    String TOPIC = "topics";

    /** The kafak metadata broker list. */
    String KAFAK_METADATA_BROKER_LIST = "metadata.broker.list";

    /** The group id. */
    String GROUP_ID = "group.id";

    /** The driver class. */
    String DRIVER_CLASS = "driver-class";

    /** The streaming checkpoint. */
    String STREAMING_CHECKPOINT = "streaming.checkpoint";

    /** The spark external block store url. */
    String SPARK_EXTERNAL_BLOCK_STORE_URL = "spark.externalBlockStore.url";

    /** The spark serializer. */
    String SPARK_SERIALIZER = "spark.serializer";

    /** The master url. */
    String MASTER_URL = "master.url";

    /** The app name. */
    String APP_NAME = "app.name";

    /** The spark external block store base dir. */
    String SPARK_EXTERNAL_BLOCK_STORE_BASE_DIR = "spark.externalBlockStore.baseDir";

    /** The file url. */
    String FILE_URL = "file.path";

    /** The header enabled. */
    String HEADER_ENABLED = "header";

    /** The infer schema. */
    String INFER_SCHEMA = "inferSchema";

    /** The drop malformed. */
    String DROP_MALFORMED = "drop-malformed";

    /** The date format. */
    String DATE_FORMAT = "dateFormat";

    /** The quote. */
    String QUOTE = "quote";

    /** The url sep. */
    String URL_SEP = "//";

    /** The data format. */
    String DATA_FORMAT = "data.format";

    String DELIMITER = "delimiter";

    /** The partition columns. */
    String PARTITION_COLUMNS = "partition.columns";

    /** The connection url. */
    String CONNECTION_URL = "jdbc.connection.url";

    /** The step indicator. */
    String STEP_INDICATOR = ":";

    /** The separator. */
    String SEPARATOR = File.separator;

    /** The ingress. */
    String INGRESS = "ingress";

    /** The egress. */
    String EGRESS = "egress";

    /** The step. */
    String STEP = "step";

    /** The dot. */
    String DOT = ".";

    /** The uri seperator. */
    String URI_SEPERATOR = "://";

    /** The file uri seperator. */
    String FILE_URI_SEPERATOR = ":///";

    /**
     * The Enum ConfigType.
     */
    enum ConfigType {

        /** The step. */
        STEP("step"),
        /** The input. */
        INPUT("ingress"),
        /** The output. */
        OUTPUT("egress");

        /** The path. */
        private final String path;

        /**
         * Instantiates a new config type.
         *
         * @param path
         *            the path
         */
        ConfigType(final String path) {
            this.path = path;
        }

        /**
         * Gets the path.
         *
         * @return the path
         */
        public String getPath() {
            return path;
        }
    }
}

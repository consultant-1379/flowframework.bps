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

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * TransformerUtils is a utility class for schema mapping.
 */
public class TransformerUtils {

    /** a schema mapping function. */
    public static Function<StructField, String> toString = new Function<StructField, String>() {
        @Override
        public String apply(final StructField input) {
            return input.name() + " " + input.dataType().simpleString();
        }

    };

    /** A DDL generation function. */
    public static Function<StructType, String> buldeDef = new Function<StructType, String>() {
        @Override
        public String apply(final StructType input) {
            LOG.trace("Entering the apply method ");
            final StructField[] fields = input.fields();
            final StringBuffer buff = new StringBuffer();
            for (int i = 0; i < fields.length; i++) {
                if (i == 0) {
                    buff.append("(");
                }
                buff.append(toString.apply(fields[i]));
                if ((i + 1) == fields.length) {
                    buff.append(")");
                } else {
                    buff.append(",");
                }
            }
            return buff.toString();
        }

    };

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(TransformerUtils.class);

    private TransformerUtils() {

    }

    /**
     * Returns a DDL part to the caller.
     *
     * @param stuct
     *            the stuct
     * @param split
     *            the split
     * @return the string
     */
    public static String transform(final StructType stuct, final String[] split) {
        LOG.trace("Entering the transform method ");
        Preconditions.checkArgument(stuct != null, "Invalid schema, schema cannot be null");
        final String tableDef = filterSchema(stuct, split);
        if (split == null) {
            return tableDef;
        }
        final String partDef = filterPartitions(stuct, split);
        LOG.trace("Existing from the method ");
        return tableDef + " partitioned by " + partDef;

    }

    /**
     * Builds the DDL.
     *
     * @param stuct
     *            the stuct
     * @param split
     *            the split
     * @param tableName
     *            the table name
     * @param format
     *            the format
     * @return the string
     */
    public static String buildDDL(final StructType stuct, final String[] split, final String tableName, final String format) {
        return "CREATE TABLE IF NOT EXISTS " + tableName + " " + transform(stuct, split) + " STORED AS " + format;
    }

    /**
     * This method filter out any partitions requested and create DDL and return to the caller. if the partitions not defined full schema will be
     * listed
     *
     * @param stuct
     *            schema from the spark {@link DataFrame}
     * @param split
     *            is the partitions split from the schema
     * @return a DDL statement back to the caller
     */
    public static String filterSchema(final StructType stuct, final String[] split) {
        LOG.trace("Entering the filterSchema method ");
        Preconditions.checkArgument(stuct != null, "Invalid schema, schema cannot be null");
        final StructField[] fields = stuct.fields();
        if (split == null) {
            return buldeDef.apply(stuct);
        }
        StructType newSet = new StructType();
        final PartFixer finder = new PartFixer(split);
        for (int i = 0; i < fields.length; i++) {
            if (!finder.apply(fields[i])) {
                newSet = newSet.add(fields[i]);
            }
        }
        LOG.trace("Existing from the filterSchema method ");
        return buldeDef.apply(newSet);
    }

    /**
     * A filter for the raw schema.
     *
     * @param struct
     *            the struct
     * @param split
     *            is the partitions split from the schema
     * @return a DDL statement back to the caller
     */
    public static String filterPartitions(final StructType struct, final String[] split) {
        LOG.trace("Entering the filterPartitions method ");
        Preconditions.checkArgument(struct != null, "Invalid schema, schema cannot be null");
        final StructField[] fields = struct.fields();
        StructType newSet = new StructType();
        for (final String par : split) {
            for (int i = 0; i < fields.length; i++) {
                if (par.equalsIgnoreCase(fields[i].name())) {
                    newSet = newSet.add(fields[i]);
                    break;
                }
            }
        }
        LOG.trace("Existing from the filterPartitions method ");
        return buldeDef.apply(newSet);
    }

    /**
     * Function for the schema search.
     */
    private static class PartFixer implements Function<StructField, Boolean> {

        /** The part fixer. */
        private final String[] partFixer;

        /**
         * Instantiates a new part fixer.
         *
         * @param parts
         *            the parts
         */
        PartFixer(final String[] parts) {
            this.partFixer = parts;
        }

        @Override
        public Boolean apply(final StructField input) {
            for (int i = 0; i < partFixer.length; i++) {
                if (input.name().equalsIgnoreCase(partFixer[i].trim())) {
                    return true;
                }
            }
            return false;
        }
    }
}

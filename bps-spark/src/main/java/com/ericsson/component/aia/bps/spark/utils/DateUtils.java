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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.service.configuration.BpsDataStreamsConfigurer;

/**
 * DateUtils is a utility class for all date related operations.
 */
public class DateUtils {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsDataStreamsConfigurer.class);

    private DateUtils() {

    }

    // TODO: not used
    /**
     *
     *
     * Gets the dates.
     *
     * @param str_date
     *            the str date
     * @param end_date
     *            the end date
     * @param format
     *            the format
     * @return the dates
     */
    public static String getDates(final String str_date, final String end_date, final String format) {
        LOG.trace("Entering the getDates method ");
        final List<Date> dates = new ArrayList<Date>();
        final DateFormat formatter;
        Date startDate = null;
        Date endDate = null;

        try {
            formatter = new SimpleDateFormat("dd/MM/yyyy");
            startDate = formatter.parse(str_date);
            endDate = formatter.parse(end_date);
        } catch (final ParseException expection) {
            LOG.error("Exception while processing getDates. Exception is: ", expection);
            throw new IllegalArgumentException(expection);
        }

        final long interval = 24 * 1000 * 60 * 60; // 1 hour in millis
        final long endTime = endDate.getTime(); // create your endtime here,
        // possibly using Calendar or
        // Date
        long curTime = startDate.getTime();
        while (curTime <= endTime) {
            dates.add(new Date(curTime));
            curTime += interval;
        }
        LOG.trace("Returning the getDates method ");
        return StringUtils.join(dates, ',');
    }
}

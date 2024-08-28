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
package com.ericsson.component.aia.bps.core.service.configuration.partition;

import java.util.Properties;

/**
 * Partition interface defines logic to distributes data records
 *
 * @param <O>
 *            data type to write
 */
public interface BpsPartition<O> {

    /**
     * Init operation configures partitioning strategy properties.
     *
     * @param props
     *            Partition strategy configurations
     */
    void init(Properties props);

    /**
     * Write operation defines logic for writing output data.
     *
     * @param dataSet
     *            Input data set
     * @param path
     *            Destination location
     */
    void write(O dataSet, String path);
}

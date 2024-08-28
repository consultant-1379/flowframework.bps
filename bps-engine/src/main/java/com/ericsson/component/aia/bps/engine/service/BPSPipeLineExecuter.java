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
package com.ericsson.component.aia.bps.engine.service;

import static com.google.common.base.Preconditions.checkArgument;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.pipe.BpsPipe;
import com.ericsson.component.aia.bps.engine.parser.ExecuterHelper;

/**
 * This class is responsible for loading the flow xml file, create pipe and execute it.
 *
 */
public class BPSPipeLineExecuter extends ExecuterHelper {

    private static final long serialVersionUID = -7909556800762996458L;

    private static final Logger LOGGER = LoggerFactory.getLogger(BPSPipeLineExecuter.class);

    /**
     * This method is starting point of execution
     *
     * @param args
     *            command line arguments
     */
    public static void main(final String[] args) {
        try {
            checkArgument(args != null && args.length == 1, "Usage java <BPSPipeLineExecuter> <flow xml path>");
            checkArgument(args[0] != null && args[0].trim().length() > 0, "Please provide valid flow xml path argument");
            final BPSPipeLineExecuter instance = new BPSPipeLineExecuter();
            final BpsPipe pipe = instance.init(args[0]);
            LOGGER.info("Initialized Pipe-line successfully, executing pipe-line now!!!");
            pipe.execute();
            LOGGER.info("Pipe-line executed successfully");
            pipe.cleanUp();
            LOGGER.info("Pipe-line clean up successful");
        } catch (final Exception exp) {
            LOGGER.error("Exception occurred while executing application, reason \n", exp);
        }
    }
}

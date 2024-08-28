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
package com.ericsson.component.aia.bps.engine.parser;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;

import com.ericsson.component.aia.bps.core.pipe.BpsPipe;

/**
 * This class will read flow xml and configures the pipe line to be executed.
 */
public class ExecuterHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * This method will parse the flowxml and build an pipe line for execution
     *
     * @param flowXML
     *            to read i/o and step configuration
     * @return an pipe line for execution
     *
     * @throws FileNotFoundException
     *             if flow xml is not found
     */
    public BpsPipe init(final String flowXML) throws FileNotFoundException {
        final File file = new File(flowXML);
        checkArgument(file.exists() && file.isFile(), "Invalid flow xml file path, please provide valid path");
        return BpsModelParser.parseFlow(new FileInputStream(file));
    }

}

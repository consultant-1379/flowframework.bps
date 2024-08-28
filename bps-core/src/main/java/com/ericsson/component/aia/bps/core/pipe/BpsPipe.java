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
package com.ericsson.component.aia.bps.core.pipe;

import java.io.Serializable;
import java.util.List;

import com.ericsson.oss.itpf.datalayer.modeling.metamodel.gen.fbp_flow.PathType;

/**
 * Pipe interface executes the operations in the pipeline by first translating them to the flow representation using the eventflow-api and then
 * submitting them to a service for execution.
 */
public interface BpsPipe extends Serializable {

    // void config(InputAdapters inputs, Step step, OutputAdapters output);

    /**
     * Execute operation runs a pipeline job.
     */
    void execute();

    /**
     * Sets the flow.
     *
     * @param path
     *            the new flow
     */
    void setFlow(List<PathType> path);

    /**
     * Sets the name.
     *
     * @param name
     *            the new name
     */
    void setName(String name);

    /**
     * Sets the namespace.
     *
     * @param nameSpace
     *            the new namespace
     */
    void setNAMESPACE(String nameSpace);

    /**
     * Sets the version.
     *
     * @param version
     *            the new version
     */
    void setVERSION(String version);

    /**
     * This method will cleanup all the resources allocated for this pipe.
     */
    void cleanUp();
}

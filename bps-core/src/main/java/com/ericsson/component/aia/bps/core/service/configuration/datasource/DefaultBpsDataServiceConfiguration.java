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
package com.ericsson.component.aia.bps.core.service.configuration.datasource;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants.ConfigType;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.utils.IOUtil;
import com.ericsson.component.aia.bps.core.validation.BpsConfigurationValidation;

/**
 * DefaultInputs class holds/refer different bps data source.
 */
public class DefaultBpsDataServiceConfiguration extends BpsConfigurationValidation implements BpsDataSourceConfiguration {

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBpsDataServiceConfiguration.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 6691622855599610509L;

    /** The context name. */
    private String dataSourceContextName;

    /** The properties. */
    private Properties properties;

    /**
     * configure method initializes different bps data sources after mandatory fields check for a stream.
     *
     * @param dataSourceContextName
     *            the context name
     * @param props
     *            the props
     */
    @Override
    public void configure(final String dataSourceContextName, final Properties props) {
        LOG.trace("Entering the configure method");
        final Set<String> requiredProps = IOUtil.getMandatoryProps(dataSourceContextName, props, ConfigType.INPUT);

        checkArgument(requiredProps != null, "No configuration done for the input adapter " + IOURIS.getUriType(props));

        checkForMandatoryProps(requiredProps, props);
        this.properties = props;
        this.dataSourceContextName = dataSourceContextName;
        LOG.trace("Existing the configure method");
    }

    @Override
    public String getDataSourceContextName() {
        return dataSourceContextName;
    }

    @Override
    public Properties getDataSourceConfiguration() {
        return properties;
    }

    @Override
    public String toString() {
        return "[InputName=" + IOURIS.getUriType(properties) + " contextName=" + dataSourceContextName + "]";
    }
}

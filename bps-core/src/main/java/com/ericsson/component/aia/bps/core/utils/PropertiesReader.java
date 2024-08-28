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
package com.ericsson.component.aia.bps.core.utils;

import static com.ericsson.component.aia.bps.core.common.Constants.DOT;
import static com.ericsson.component.aia.bps.core.common.Constants.EGRESS;
import static com.ericsson.component.aia.bps.core.common.Constants.INGRESS;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertiesReader is a utility class for all properties related operations.
 */
public class PropertiesReader {

    /** The Constant propertiesMap. */
    private static final Map<String, Set<String>> propertiesMap = new HashMap<>();

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesReader.class);

    /** The instance. */
    private static PropertiesReader instance;

    /** The props. */
    private Properties props;

    /**
     * Instantiates a new properties reader.
     */
    private PropertiesReader() {

        setProperties("io.properties", false);
        setProperties("step.properties", true);
    }

    /**
     * Sets the properties.
     *
     * @param filename
     *            the filename
     * @param isStep
     *            the is step
     */
    private void setProperties(final String filename, final boolean isStep) {
        LOG.trace("Entering the method setProperties(String filename, boolean isStep)");
        props = new Properties();
        InputStream input = null;

        input = getClass().getClassLoader().getResourceAsStream(filename);

        if (input == null) {
            LOG.error("Sorry, unable to find " + filename);
            return;
        }

        try {
            props.load(input);
            input.close();
        } catch (final IOException ex) {
            LOG.error("Exception while processing setProperties(String filename, boolean isStep): " + ex.getMessage());
        } finally {
            if (null != input) {
                try {
                    input.close();
                } catch (final IOException e) {
                    LOG.error("Exception while processing finally block in setProperties(String filename, boolean isStep): " + e.getMessage());
                }
            }
        }

        final Enumeration<?> enumeration = props.propertyNames();

        while (enumeration.hasMoreElements()) {

            final String key = (String) enumeration.nextElement();
            final String value = props.getProperty(key).trim();

            if (isStep) {
                setStepProperties(key, value);
            } else {
                setIOProperties(key, value);
            }
        }
    }

    /**
     * Sets the step properties.
     *
     * @param key
     *            the key
     * @param value
     *            the value
     */
    private void setStepProperties(String key, final String value) {
        final int index = key.indexOf(DOT) + 1;
        final String tmp = key.substring(index);

        key = tmp.substring(0, tmp.indexOf(DOT));

        if (propertiesMap.containsKey(key)) {
            final Set<String> propsSet = propertiesMap.get(key);
            propsSet.add(value);
            propertiesMap.put(key, propsSet);

        } else {

            final Set<String> propsSet = new HashSet<>();
            propsSet.add(value);
            propertiesMap.put(key, propsSet);
        }
    }

    /**
     * Sets the IO properties.
     *
     * @param key
     *            the key
     * @param value
     *            the value
     */
    private void setIOProperties(String key, final String value) {
        final int index = key.indexOf(DOT) + 1;
        final String tmp = key.substring(index);

        if (key.startsWith(INGRESS + DOT)) {

            key = INGRESS + DOT + tmp.substring(0, tmp.indexOf(DOT));
        } else if (key.startsWith(EGRESS + DOT)) {
            key = EGRESS + DOT + tmp.substring(0, tmp.indexOf(DOT));
        }

        if (propertiesMap.containsKey(key)) {
            final Set<String> propsSet = propertiesMap.get(key);
            propsSet.add(value);
            propertiesMap.put(key, propsSet);

        } else {

            final Set<String> propsSet = new HashSet<>();
            propsSet.add(value);
            propertiesMap.put(key, propsSet);
        }
    }

    /**
     * Gets the single instance of PropertiesReader.
     *
     * @return single instance of PropertiesReader
     */
    public static PropertiesReader getInstance() {
        if (instance == null) {

            synchronized (PropertiesReader.class) {
                if (instance == null) {

                    instance = new PropertiesReader();
                }
            }
        }
        return instance;
    }

    /**
     * Gets the configs.
     *
     * @param property
     *            the property
     * @return the configs
     */
    public Set<String> getConfigs(final String property) {

        return propertiesMap.get(property);
    }

    /**
     * Gets the properties.
     *
     * @param defaultPropsPath
     *            the default props path
     * @param customProps
     *            the custom props
     * @return the properties
     */
    public static CompositeConfiguration getProperties(final String defaultPropsPath, final Properties customProps) {

        final CompositeConfiguration config = new CompositeConfiguration();
        try {
            config.addConfiguration(new PropertiesConfiguration(defaultPropsPath + ".properties"));
        } catch (final ConfigurationException exp) {
            LOG.error("Exception while processing getProperties(String defaultPropsPath, Properties customProps): " + exp.getMessage());
            throw new IllegalArgumentException(exp);
        }

        for (final String key : customProps.stringPropertyNames()) {
            final String value = customProps.getProperty(key);
            config.addProperty(key, value);
        }

        return config;
    }

    /**
     * Gets the property as string.
     *
     * @param props
     *            the props
     * @return the property as string
     */
    public static String getPropertyAsString(final Properties props) {

        final StringBuilder sbd = new StringBuilder("{");
        final Enumeration<?> enumeration = props.propertyNames();

        while (enumeration.hasMoreElements()) {

            final String key = (String) enumeration.nextElement();
            final String value = props.getProperty(key).trim();

            sbd.append("(Key:" + key + "->Value:" + value + ")");
        }

        sbd.append("}");

        return sbd.toString();
    }
}
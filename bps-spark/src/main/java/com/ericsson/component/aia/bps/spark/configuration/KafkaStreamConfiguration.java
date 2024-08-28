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
package com.ericsson.component.aia.bps.spark.configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.validation.BpsConfigurationValidation;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * The Class KafkaStreamConfiguration.
 */
public class KafkaStreamConfiguration extends BpsConfigurationValidation implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3064117827490530077L;

    /** The minimum required keys. */
    private static Set<String> minimumRequiredKeys = new HashSet<>();

    /** The brokers. */
    private String brokers;

    /** The topics. */
    private String topics;

    /** The key class. */
    private String keyClass;

    /** The value class. */
    private String valueClass;

    /** The k decoder. */
    private String kDecoder;

    /** The value decoder. */
    private String valueDecoder;

    /** The win len. */
    private String winLen;

    /** The slide win. */
    private String slideWin;

    /** The topics set. */
    private final Set<String> topicsSet = new HashSet<String>();

    /** The kafka params. */
    private final Map<String, String> kafkaParams = new HashMap<String, String>();

    /** The uri. */
    private String uri = null;

    /** The name. */
    private String name;

    /** The group id. */
    private final String groupId;

    static {
        minimumRequiredKeys.add(Constants.URI);
        minimumRequiredKeys.add(Constants.KAFAK_METADATA_BROKER_LIST);
        minimumRequiredKeys.add(Constants.WIN_LENGTH);
        minimumRequiredKeys.add(Constants.SLIDE_WIN_LENGTH);
        minimumRequiredKeys.add(Constants.GROUP_ID);
    }

    /**
     * Instantiates a new kafka stream configuration.
     *
     * @param configuration
     *            the configuration
     */
    public KafkaStreamConfiguration(final Properties configuration) {
        checkForMandatoryProps(minimumRequiredKeys, configuration);
        uri = configuration.getProperty(Constants.URI);
        brokers = configuration.getProperty(Constants.KAFAK_METADATA_BROKER_LIST);
        topics = configuration.getProperty(Constants.TOPIC);
        topicsSet.addAll(transformExtensionPoints(topics, Constants.KAFKA_URI));
        winLen = configuration.getProperty(Constants.WIN_LENGTH);
        slideWin = configuration.getProperty(Constants.SLIDE_WIN_LENGTH);
        groupId = configuration.getProperty(Constants.GROUP_ID);
        kafkaParams.put(Constants.KAFAK_METADATA_BROKER_LIST, brokers);
        kafkaParams.put(Constants.GROUP_ID, groupId);
    }

    /**
     * Transform extension points.
     *
     * @param extensionPoint
     *            the e P
     * @param indexer
     *            the indexer
     * @return the collection
     */
    private Collection<String> transformExtensionPoints(final String extensionPoint, final String indexer) {
        final List<String> splitToList = Arrays.asList(extensionPoint.split(","));
        final Collection<String> transform = Collections2.transform(splitToList, new Function<String, String>() {
            @Override
            public String apply(final String arg0) {
                return (arg0.contains(indexer) ? arg0.replace(indexer, "") : arg0).trim();
            }
        });
        return transform;
    }

    @Override
    @SuppressWarnings({ "PMD.CyclomaticComplexity", "PMD.ModifiedCyclomaticComplexity", "PMD.StdCyclomaticComplexity", "PMD.ExcessiveMethodLength",
            "PMD.NPathComplexity" })
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final KafkaStreamConfiguration other = (KafkaStreamConfiguration) obj;
        if (brokers == null) {
            if (other.brokers != null) {
                return false;
            }
        } else if (!brokers.equals(other.brokers)) {
            return false;
        }
        if (kDecoder == null) {
            if (other.kDecoder != null) {
                return false;
            }
        } else if (!kDecoder.equals(other.kDecoder)) {
            return false;
        }
        if (keyClass == null) {
            if (other.keyClass != null) {
                return false;
            }
        } else if (!keyClass.equals(other.keyClass)) {
            return false;
        }
        if (slideWin == null) {
            if (other.slideWin != null) {
                return false;
            }
        } else if (!slideWin.equals(other.slideWin)) {
            return false;
        }
        if (topics == null) {
            if (other.topics != null) {
                return false;
            }
        } else if (!topics.equals(other.topics)) {
            return false;
        }
        if (uri == null) {
            if (other.uri != null) {
                return false;
            }
        } else if (!uri.equals(other.uri)) {
            return false;
        }
        if (valueClass == null) {
            if (other.valueClass != null) {
                return false;
            }
        } else if (!valueClass.equals(other.valueClass)) {
            return false;
        }
        if (valueDecoder == null) {
            if (other.valueDecoder != null) {
                return false;
            }
        } else if (!valueDecoder.equals(other.valueDecoder)) {
            return false;
        }
        if (winLen == null) {
            if (other.winLen != null) {
                return false;
            }
        } else if (!winLen.equals(other.winLen)) {
            return false;
        }
        return true;
    }

    @SuppressWarnings({ "PMD.CyclomaticComplexity", "PMD.ModifiedCyclomaticComplexity", "PMD.StdCyclomaticComplexity", "PMD.ExcessiveMethodLength",
            "PMD.NPathComplexity" })
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokers == null) ? 0 : brokers.hashCode());
        result = prime * result + ((kDecoder == null) ? 0 : kDecoder.hashCode());
        result = prime * result + ((keyClass == null) ? 0 : keyClass.hashCode());
        result = prime * result + ((slideWin == null) ? 0 : slideWin.hashCode());
        result = prime * result + ((topics == null) ? 0 : topics.hashCode());
        result = prime * result + ((uri == null) ? 0 : uri.hashCode());
        result = prime * result + ((valueClass == null) ? 0 : valueClass.hashCode());
        result = prime * result + ((valueDecoder == null) ? 0 : valueDecoder.hashCode());
        result = prime * result + ((winLen == null) ? 0 : winLen.hashCode());
        return result;
    }

    /**
     * Gets the brokers.
     *
     * @return the brokers
     */
    public String getBrokers() {
        return brokers;
    }

    /**
     * Gets the group id.
     *
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * Gets the kafka params.
     *
     * @return the kafkaParams
     */
    public Map<String, String> getKafkaParams() {
        return kafkaParams;
    }

    /**
     * Gets the k decoder.
     *
     * @return the kDecoder
     */
    public String getkDecoder() {
        return kDecoder;
    }

    /**
     * Gets the key class.
     *
     * @return the keyClass
     */
    public String getKeyClass() {
        return keyClass;
    }

    /**
     * Gets the slide win.
     *
     * @return the slideWin
     */
    public String getSlideWin() {
        return slideWin;
    }

    /**
     * Gets the topics.
     *
     * @return the topics
     */
    public String getTopics() {
        return topics;
    }

    /**
     * Gets the topics set.
     *
     * @return the topicsSet
     */
    public Set<String> getTopicsSet() {
        return topicsSet;
    }

    /**
     * Gets the value class.
     *
     * @return the valueClass
     */
    public String getValueClass() {
        return valueClass;
    }

    /**
     * Gets the value decoder.
     *
     * @return the valueDecoder
     */
    public String getValueDecoder() {
        return valueDecoder;
    }

    /**
     * Gets the win len.
     *
     * @return the winLen
     */
    public String getWinLen() {
        return winLen;
    }

    /**
     * Sets the brokers.
     *
     * @param brokers
     *            the brokers to set
     */
    public void setBrokers(final String brokers) {
        this.brokers = brokers;
    }

    /**
     * Sets the k decoder.
     *
     * @param kDecoder
     *            the kDecoder to set
     */
    public void setkDecoder(final String kDecoder) {
        this.kDecoder = kDecoder;
    }

    /**
     * Sets the key class.
     *
     * @param keyClass
     *            the keyClass to set
     */
    public void setKeyClass(final String keyClass) {
        this.keyClass = keyClass;
    }

    /**
     * Sets the slide win.
     *
     * @param slideWin
     *            the slideWin to set
     */
    public void setSlideWin(final String slideWin) {
        this.slideWin = slideWin;
    }

    /**
     * Sets the topics.
     *
     * @param topics
     *            the topics to set
     */
    public void setTopics(final String topics) {
        this.topics = topics;
    }

    /**
     * Sets the uri.
     *
     * @param uri
     *            the uri to set
     */
    public void setUri(final String uri) {
        this.uri = uri;
    }

    /**
     * Sets the value class.
     *
     * @param valueClass
     *            the valueClass to set
     */
    public void setValueClass(final String valueClass) {
        this.valueClass = valueClass;
    }

    /**
     * Sets the value decoder.
     *
     * @param valueDecoder
     *            the valueDecoder to set
     */
    public void setValueDecoder(final String valueDecoder) {
        this.valueDecoder = valueDecoder;
    }

    /**
     * Sets the win len.
     *
     * @param winLen
     *            the winLen to set
     */
    public void setWinLen(final String winLen) {
        this.winLen = winLen;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the uri.
     *
     * @return the uri
     */
    public String getUri() {
        return uri;
    }
}

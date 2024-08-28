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
package com.ericsson.component.aia.common.service.loader;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.common.service.GenericService;
import com.ericsson.component.aia.common.service.exception.ServiceNotFoundException;

/**
 * This class provides generic method to get service implementation of any service extending {@link GenericService} based on service context name
 * provided in the service implementation
 */
public final class GenericServiceLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericServiceLoader.class);

    private static final Map<Class<? extends GenericService>, Map<String, Class<? extends GenericService>>> cache = new HashMap<>();

    private GenericServiceLoader() {
    }

    /**
     * This method will load all the service implementation of the service class type provided as parameter into the cache
     *
     * @param clzz
     *            loads all service implementation of the service class type into the cache
     */
    private static void load(final Class<? extends GenericService> clzz) {
        LOGGER.trace("Loading service implemation for service type {}", clzz.getName());
        Map<String, Class<? extends GenericService>> map = cache.get(clzz);
        if (map == null) {
            map = new HashMap<String, Class<? extends GenericService>>();
            cache.put(clzz, map);
        }
        final ServiceLoader<? extends GenericService> load = ServiceLoader.load(clzz);
        final Iterator<? extends GenericService> iterator = load.iterator();
        while (iterator.hasNext()) {
            final GenericService next = iterator.next();
            map.put(next.getServiceContextName(), next.getClass());
        }
        LOGGER.trace("Loading service implemation successful for service type {}", clzz.getName());
    }

    /**
     * This method will return the service implementation of the service type and service context name provided as parameter
     *
     * @param serviceType
     *            the service type of the service implementation to return
     * @param contextName
     *            the service context name of the service implementation to return
     * @return service implementation of service type with service context name provided as parameter
     */
    public static GenericService getService(final Class<? extends GenericService> serviceType, final String contextName) {
        LOGGER.trace("Trying to find service implemation for service type={} with service context name={}", serviceType.getName(), contextName);
        final Map<String, Class<? extends GenericService>> map = cache.get(serviceType);
        if (map == null) {
            load(serviceType);
        }
        final Class<? extends GenericService> serviceImpl = cache.get(serviceType).get(contextName);
        if (serviceImpl == null) {
            throw new ServiceNotFoundException(String.format("Requested implementation of service type  %s with context name %s not found",
                    serviceType.getName(), contextName));
        }
        try {
            LOGGER.trace("Found service implemation for service type={} with service context name={}", serviceType.getName(), contextName);
            return serviceImpl.newInstance();
        } catch (InstantiationException | IllegalAccessException exp) {
            throw new ServiceNotFoundException(String.format("Unable to initialized instance of %s with for the context name %s.",
                    serviceImpl.getCanonicalName(), contextName), exp.getCause());
        }
    }
}

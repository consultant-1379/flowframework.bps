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
package com.ericsson.component.aia.bps.engine.service.spark;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.runners.Parameterized;
import org.junit.runners.model.RunnerScheduler;

/**
 * An extension of the JUnit {@link Parameterized} runner, which executes the tests for each parameter set concurrently.
 * <p>
 * You can specify the maximum number of parallel test threads using the system property <code>maxParallelTestThreads</code>. If this system property
 * is not specified, the maximum number of test threads will be the number of {@link Runtime#availableProcessors() available processors}.
 */
public class Parallelized extends Parameterized {

    /**
     * The Class ThreadPoolScheduler.
     */
    private static class ThreadPoolScheduler implements RunnerScheduler {

        /** The executor. */
        private ExecutorService executor;

        /**
         * Instantiates a new thread pool scheduler.
         */
        ThreadPoolScheduler() {
            final String threads = System.getProperty("junit.parallel.threads", "16");
            final int numThreads = Integer.parseInt(threads);
            executor = Executors.newFixedThreadPool(numThreads);
        }

        /**
         * Finished.
         */
        @Override
        public void finished() {
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.MINUTES);
            } catch (final InterruptedException exc) {
                throw new RuntimeException(exc);
            }
        }

        /**
         * Schedule.
         *
         * @param childStatement
         *            the child statement
         */
        @Override
        public void schedule(final Runnable childStatement) {
            executor.submit(childStatement);
        }
    }

    /**
     * Instantiates a new parallelized.
     *
     * @param klass
     *            the klass
     * @throws Throwable
     *             the throwable
     */
    public Parallelized(final Class<?> klass) throws Throwable {
        super(klass);
        setScheduler(new ThreadPoolScheduler());
    }
}
package org.sakaiproject.nakamura.lite.soak;

import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A controller for a multi threaded test.
 * 
 * @author ieb
 * 
 */
public abstract class AbstractSoakController {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSoakController.class);
    private double singleThreadRate;
    private int totalOperations;

    public AbstractSoakController(int totalOperations) {
        this.totalOperations = totalOperations;
    }

    public void launchSoak(int nthreads) throws ClientPoolException, StorageClientException,
            AccessDeniedException {
        LOGGER.info("|Threads|Time s|Throughput|Throughput per thread| Concurrency| Efficiency|");
        for (int tr = 1; tr <= nthreads; tr++) {
            long s = System.currentTimeMillis();
            Thread[] threads = new Thread[tr];
            for (int t = 0; t < tr; t++) {
                threads[t] = new Thread(getRunnable(tr));
                threads[t].start();
            }
            for (int t = 0; t < tr; t++) {
                try {
                    threads[t].join();
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }

            long e = System.currentTimeMillis();
            double t = (e - s) / ((double) 1000);
            logRate(t, tr);
        }

    }

    protected void logRate(double t, int currentThreads) {
        double rate = ((double) totalOperations) / t;
        double ratePerThread = ((double) totalOperations) / (((double) currentThreads) * t);
        if (currentThreads == 1) {
            singleThreadRate = rate;
        }
        double speedup = rate / singleThreadRate;
        double efficiency = 100 * speedup / ((double) currentThreads);
        LOGGER.info("| {}| {}| {}| {}| {}| {}%|", new Object[] { currentThreads, t, rate,
                ratePerThread, speedup, efficiency });
    }

    protected abstract Runnable getRunnable(int tr) throws ClientPoolException,
            StorageClientException, AccessDeniedException;
}

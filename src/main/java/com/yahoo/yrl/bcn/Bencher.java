package com.yahoo.yrl.bcn;

import com.google.common.util.concurrent.RateLimiter;

import java.util.TimerTask;
import java.util.Timer;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class Bencher {
    static Logger LOG = LoggerFactory.getLogger(Bencher.class);

    IntegerDistribution distribution;
    int seconds = 60;
    int rate = 1000;
    byte[] data = new byte[100];
    final Adaptor adaptor;
    BenchStats stats;

    Bencher(Adaptor a) {
        adaptor = a;
        if (a.getNumShards() > 1) {
            distribution = new UniformIntegerDistribution(0, a.getNumShards()-1);
        } else {
            distribution = new UniformIntegerDistribution(0, 2) { //hack
                    public int sample() {
                        return 0;
                    }
                };
        }
        stats = new BenchStats();
    }

    Bencher setTime(int seconds) {
        this.seconds = seconds;
        return this;
    }

    Bencher setRate(int rate) {
        this.rate = rate;
        return this;
    }

    Bencher setData(byte[] data) {
        this.data = data;
        return this;
    }

    void runReads(int numReads) throws Exception {
        final List<Integer> shards = new ArrayList<Integer>(adaptor.getNumShards());
        for (int i = 0; i < adaptor.getNumShards(); i++) {
            shards.add(i);
        }
        Collections.shuffle(shards);

        ExecutorService executor = Executors.newFixedThreadPool(numReads);
        List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
        
        final AtomicBoolean explicitShutdown = new AtomicBoolean(false);
        for (int i = 0; i < numReads; i++) {
            tasks.add(new Callable<Long>() {
                    public Long call() {
                        try {
                            final long start = System.nanoTime();
                            final AtomicLong end = new AtomicLong(0);
                            adaptor.readEntries(shards.remove(0), new Adaptor.Reader() {
                                    public void entryRead(byte[] data) {
                                        stats.recordRead();
                                    }
                                    public void done() {
                                        end.set(System.nanoTime());
                                    }
                                });
                            return (end.get()-start)/1000000;
                        } catch (Exception e) {
                            if (!explicitShutdown.get()) {
                                LOG.error("Error completing read", e);
                            }
                            return Long.MAX_VALUE;
                        }
                    }
                });
        }
        List<Future<Long>> res = executor.invokeAll(tasks, seconds, TimeUnit.SECONDS);
        stats.finish();

        explicitShutdown.set(true);
        executor.shutdownNow();

        long tot = 0;
        int count = 0;
        for (Future<Long> l : res) {
            if (l.isDone() && !l.isCancelled()) {
                tot += l.get();
                count++;
            }
        }

        stats.logReadTimeSeries();
        stats.logReadAverages();
        if (count > 0) {
            LOG.info("Avg read time per completed shard: {} ms", tot/count);
        }
    }

    void runWrites() throws Exception {
        RateLimiter limiter = RateLimiter.create(rate);

        final AtomicBoolean done = new AtomicBoolean(false);
        Timer t = new Timer("FinisherTimer", true);
        t.schedule(new TimerTask() {
                public void run() {
                    stats.finish();
                    done.set(true);
                }
            }, seconds*1000);

        try {
            while (!done.get()) {
                limiter.acquire();
                
                long start = System.nanoTime();
                adaptor.addEntry(distribution.sample(), data);
                long end = System.nanoTime();
                stats.recordAdd(start, end);
            }
        } finally {
            t.cancel();
        }        
        stats.logWriteTimeSeries();
        stats.logWriteAverages();
    }
}

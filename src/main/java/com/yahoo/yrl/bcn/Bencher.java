package com.yahoo.yrl.bcn;

import com.google.common.util.concurrent.RateLimiter;

import java.util.TimerTask;
import java.util.Timer;

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
        distribution = new UniformIntegerDistribution(0, a.getNumShards()-1);
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

    void run() throws Exception {
        RateLimiter limiter = RateLimiter.create(rate);

        final AtomicBoolean done = new AtomicBoolean(false);
        Timer t = new Timer("FinisherTimer", true);
        t.schedule(new TimerTask() {
                public void run() {
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
            stats.finish();
        } finally {
            t.cancel();
        }        
        stats.logTimeSeries();
        stats.logAverages();
    }
}

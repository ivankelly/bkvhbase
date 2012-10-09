package com.yahoo.yrl.bcn;

import com.google.common.util.concurrent.RateLimiter;

import java.util.TimerTask;
import java.util.Timer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bencher {
    static Logger LOG = LoggerFactory.getLogger(Bencher.class);

    int seconds = 60;
    int rate = 1000;
    final Adaptor adaptor;

    Bencher(Adaptor a) {
        adaptor = a;
    }

    Bencher setTime(int seconds) {
        this.seconds = seconds;
        return this;
    }

    Bencher setRate(int rate) {
        this.rate = rate;
        return this;
    }

    void run() throws Exception {
        RateLimiter limiter = RateLimiter.create(rate);
        long start = System.currentTimeMillis();
        int i  = 1;

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
                
                if (i % 1000 == 0) {
                    LOG.info("Wrote {} entries", i);
                }
                i++;
                
                adaptor.addEntry(0, "sadfasfdasfasfasfasdfsafasdf".getBytes());
            }
        } finally {
            t.cancel();
        }
    }
}

package com.yahoo.yrl.bcn;

import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import com.twitter.common.stats.Rate;
import com.twitter.common.stats.Percentile;
import com.twitter.common.stats.Ratio;
import com.twitter.common.stats.Stats;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.RequestStats;
import com.twitter.common.stats.MovingAverage;
import com.twitter.common.application.ShutdownRegistry;

import com.twitter.common.stats.TimeSeries;
import com.twitter.common.stats.TimeSeriesRepository;
import com.twitter.common.stats.TimeSeriesRepositoryImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchStats {
    static Logger LOG = LoggerFactory.getLogger(BenchStats.class);

    //    private RequestStats addStats;
    private MovingAverage addTptAvg;
    private Percentile<Long> addLatPercentiles;

    private AtomicLong addsRequests;
    private AtomicLong addsTotalTime;
    private Ratio addsLatency;

    private TimeSeriesRepository sampler;
    private ShutdownRegistry.ShutdownRegistryImpl shutdown;

    BenchStats() {
        //  addStats = new RequestStats("add");
        //addAvg = MovingAverage.on(
        addsRequests = new AtomicLong(0);
        addsTotalTime = new AtomicLong(0);

        Rate addTpt = Rate.of("adds_num_requests", addsRequests).withWindowSize(1).build();
        addTptAvg = MovingAverage.of("add_tpt_avg", addTpt, 30);
        addLatPercentiles = new Percentile<Long>("add_lat_perc", 20, new double[]{95, 99});

        Stats.export(addTptAvg);
        Stats.export(Ratio.of("adds_latency",
                              Rate.of("adds_total_time", addsTotalTime).withWindowSize(1).build(),
                              addTpt));


        shutdown = new ShutdownRegistry.ShutdownRegistryImpl();
        sampler = new TimeSeriesRepositoryImpl(Stats.STAT_REGISTRY,
                Amount.of(1L, Time.SECONDS), Amount.of(1L, Time.HOURS));
        sampler.start(shutdown);
    }

    void finish() {
        shutdown.execute();
    }

    void recordAdd(long startNanos, long endNanos) {
        addsTotalTime.addAndGet((endNanos-startNanos)/1000);
        addsRequests.incrementAndGet();
        addLatPercentiles.record((endNanos-startNanos)/1000);
    }

    void logTimeSeries() {
        LOG.info("Names: {}", sampler.getAvailableSeries());
        Iterator<Number> tpt = sampler.get("adds_num_requests").getSamples().iterator();
        Iterator<Number> lat99 = sampler.get("adds_latency").getSamples().iterator();
        int i = 0;

        while (tpt.hasNext()) {
            LOG.info("{} : {} {}", new Object[] { i++, tpt.next(), lat99.next()} );
        }
    }

    void logAverages() {
        LOG.info("Add TPT Avg (last 30 seconds): {}", addTptAvg.read());
        for (Map.Entry<Double, ? extends Stat> e : addLatPercentiles.getPercentiles().entrySet()) {
            LOG.info("Add {}th %ile: {}", e.getKey(), e.getValue().read());
        }
    }
}
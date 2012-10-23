package com.yahoo.yrl.bcn;

import java.util.Map;
import java.util.Iterator;

import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import com.twitter.common.stats.Rate;
import com.twitter.common.stats.Percentile;
import com.twitter.common.stats.Ratio;
import com.twitter.common.stats.Stats;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.SampledStat;
import com.twitter.common.stats.RequestStats;
import com.twitter.common.stats.StatRegistry;
import com.twitter.common.stats.CustomStatRegistry;
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

    private MovingAverage readTptAvg;
    private AtomicLong readRequests;

    private TimeSeriesRepository sampler;
    private CustomStatRegistry registry;
    private ShutdownRegistry.ShutdownRegistryImpl shutdown;

    BenchStats() {
        //  addStats = new RequestStats("add");
        //addAvg = MovingAverage.on(
        addsRequests = new AtomicLong(0);
        addsTotalTime = new AtomicLong(0);

        readRequests = new AtomicLong(0);

        final Set<SampledStat<? extends Number>> stats = new HashSet<SampledStat<? extends Number>>();
        registry = new CustomStatRegistry(stats);

        Rate readTpt = Rate.of("read_tpt_requests", readRequests).withWindowSize(1).build();
        readTptAvg = MovingAverage.of("read_tpt_avg", readTpt, 30);
        stats.add(readTpt);
        stats.add(readTptAvg);

        Rate addTpt = Rate.of("adds_num_requests", addsRequests).withWindowSize(1).build();
        addTptAvg = MovingAverage.of("add_tpt_avg", addTpt, 30);
        stats.add(addTpt);
        stats.add(addTptAvg);

        addLatPercentiles = new Percentile<Long>("add_lat_perc", 20, new double[]{95, 99});
        registry.addPercentiles(addLatPercentiles);

        Rate addsTotalTimeRate = Rate.of("adds_total_time", addsTotalTime).withWindowSize(1).build();
        stats.add(addsTotalTimeRate);
        stats.add(Ratio.of("adds_latency",
                           addsTotalTimeRate,
                           addTpt));
        /*        SampledStat<Double> myStat = new SampledStat<Double>("Foobar", 0.0) {
            public Double doSample() {
                LOG.info("Sampling {}", readRequests.get());
                return 1.0;
            }
        };
        Stats.export(myStat);*/
        
        shutdown = new ShutdownRegistry.ShutdownRegistryImpl();
        sampler = new TimeSeriesRepositoryImpl(registry,
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

    void recordRead() {
        readRequests.incrementAndGet();
    }

    void logWriteTimeSeries() {
        Iterator<Number> tpt = sampler.get("adds_num_requests").getSamples().iterator();
        Iterator<Number> lat99 = sampler.get("adds_latency").getSamples().iterator();
        int i = 0;

        while (tpt.hasNext()) {
            LOG.info("{} : {} {}", new Object[] { i++, tpt.next(), lat99.next()} );
        }
    }

    void logWriteAverages() {
        LOG.info("Add TPT Avg (last 30 seconds): {}", addTptAvg.read());
        for (Map.Entry<Double, ? extends Stat> e : addLatPercentiles.getPercentiles().entrySet()) {
            LOG.info("Add {}th %ile: {}", e.getKey(), e.getValue().read());
        }
    }

    void logReadTimeSeries() {
        TimeSeries ts = sampler.get("read_tpt_requests");
        if (ts == null) {
            LOG.info("Not enough information collected for time series");
            return;
        }
        Iterator<Number> tpt = ts.getSamples().iterator();
        int i = 0;

        while (tpt.hasNext()) {
            LOG.info("{} : {}", new Object[] { i++, tpt.next()} );
        }
    }

    void logReadAverages() {
        LOG.info("Read TPT Avg (last 30 seconds): {}", readTptAvg.read());
    }
}
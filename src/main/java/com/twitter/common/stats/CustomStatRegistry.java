package com.twitter.common.stats;

import java.util.Set;
import java.util.Map;

public class CustomStatRegistry implements StatRegistry {
    private final Set stats;

    public CustomStatRegistry(Set stats) {
        this.stats = stats;
    }

    @Override
    public Iterable<RecordingStat<? extends Number>> getStats() {
        return stats;
    }

    public void addPercentiles(Percentile<? extends Number> p) {
        for (Map.Entry<Double, ? extends Stat> e : p.getPercentiles().entrySet()) {
            stats.add(p.getPercentile(e.getKey()));
        }
    }
}
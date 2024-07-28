package com.example.bigdata.tools;

import com.example.bigdata.model.HouseStats;
import com.example.bigdata.model.HouseStatsAccumulator;
import com.example.bigdata.model.ScoreEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class HouseStatsAggregator implements AggregateFunction<ScoreEvent, HouseStatsAccumulator, HouseStats> {
    @Override
    public HouseStatsAccumulator createAccumulator() {
        return new HouseStatsAccumulator();
    }

    @Override
    public HouseStatsAccumulator add(ScoreEvent value, HouseStatsAccumulator accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public HouseStats getResult(HouseStatsAccumulator accumulator) {
        return accumulator.getResult();
    }

    @Override
    public HouseStatsAccumulator merge(HouseStatsAccumulator a, HouseStatsAccumulator b) {
        return a.merge(b);
    }
}


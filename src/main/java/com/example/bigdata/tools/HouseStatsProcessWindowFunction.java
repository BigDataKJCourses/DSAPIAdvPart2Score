package com.example.bigdata.tools;

import com.example.bigdata.model.HouseStats;
import com.example.bigdata.model.HouseStatsResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HouseStatsProcessWindowFunction extends ProcessWindowFunction<HouseStats, HouseStatsResult, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<HouseStats> elements, Collector<HouseStatsResult> out) {
        HouseStats stats = elements.iterator().next();

        HouseStatsResult result = new HouseStatsResult(
                key,
                stats.getHowMany(),
                stats.getSumScore(),
                stats.getNoCharacters(),
                context.window().getStart(),
                context.window().getEnd());

        out.collect(result);
    }
}


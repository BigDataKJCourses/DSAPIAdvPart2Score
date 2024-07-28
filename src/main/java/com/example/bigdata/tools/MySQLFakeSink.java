package com.example.bigdata.tools;

import com.example.bigdata.model.HouseStatsResult;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MySQLFakeSink extends ProcessFunction<HouseStatsResult, String> {
    private final String command;

    public MySQLFakeSink(String command) {
        this.command = command;
    }

    @Override
    public void processElement(HouseStatsResult value, Context ctx, Collector<String> out) throws Exception {

        String resultString = replaceQueryParameters(command,
                value.getHowMany(),
                value.getSumScore(),
                value.getNoCharacters(),
                value.getHouse(),
                value.getFromAsString(),
                value.getToAsString(),
                value.getHowMany(),
                value.getSumScore(),
                value.getNoCharacters());

        out.collect(resultString);
    }

    public static String replaceQueryParameters(String command, Object... parameters) {

        for (Object parameter : parameters) {
            command = command.replaceFirst("\\?", parameter.toString());
        }

        return command;
    }
}
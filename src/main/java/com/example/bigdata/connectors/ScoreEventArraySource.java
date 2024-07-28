package com.example.bigdata.connectors;

import com.example.bigdata.model.ScoreEvent;
import com.example.bigdata.tools.ScoreEventSchema;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.google.gson.Gson;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ScoreEventArraySource implements SourceFunction<ScoreEvent> {
    private final String[] jsonStrings;
    private final int interval;
    private volatile boolean isRunning = true;
    private transient ScoreEventSchema schema;

    public ScoreEventArraySource(String[] jsonStrings, int interval) {
        this.jsonStrings = jsonStrings;
        this.interval = interval;
    }

    public ScoreEventArraySource(String[] jsonStrings) {
        this.jsonStrings = jsonStrings;
        this.interval = 1000;
    }

    @Override
    public void run(SourceContext<ScoreEvent> ctx) throws Exception {
        int index = 0;
        schema = new ScoreEventSchema();

        while (isRunning && index < jsonStrings.length) {

            ScoreEvent scoreEvent = schema.deserialize(jsonStrings[index].getBytes());

            ctx.collectWithTimestamp(scoreEvent, scoreEvent.getTs());
            TimeUnit.MILLISECONDS.sleep(interval);
            index++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}


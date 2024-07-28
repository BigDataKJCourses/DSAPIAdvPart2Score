package com.example.bigdata;

import com.example.bigdata.connectors.ScoreEventArraySource;
import com.example.bigdata.connectors.ScoreEventKafkaSource;
import com.example.bigdata.model.ScoreEvent;
import com.example.bigdata.testdata.Inputs;
import com.example.bigdata.tools.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ScoreEventsDelayAnalysis {
    public static void main(String[] args) throws Exception {

        ParameterTool properties = Properties.get(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ScoreEvent> scoreEventDS;

        if (properties.getRequired("data.input").equals("array")) {
            scoreEventDS = env.addSource(new ScoreEventArraySource(Inputs.getJsonOrderedStrings(),100));
        } else {
            scoreEventDS =
                    env.fromSource(ScoreEventKafkaSource.create(properties),
                            WatermarkStrategy.noWatermarks(), "Kafka Source");
        }

        DataStream<Long> resultDS = scoreEventDS.map(se -> 1L);

        resultDS.print();

        env.execute("Score Events Delay Analysis");
    }
}

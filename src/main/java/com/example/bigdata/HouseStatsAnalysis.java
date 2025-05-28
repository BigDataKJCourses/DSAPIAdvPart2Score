package com.example.bigdata;

import com.example.bigdata.connectors.MySQLSink;
import com.example.bigdata.connectors.ScoreEventArraySource;
import com.example.bigdata.connectors.ScoreEventKafkaSource;
import com.example.bigdata.model.HouseStats;
import com.example.bigdata.model.HouseStatsResult;
import com.example.bigdata.model.ScoreEvent;
import com.example.bigdata.testdata.Inputs;
import com.example.bigdata.tools.HouseStatsAggregator;
import com.example.bigdata.tools.HouseStatsProcessWindowFunction;
import com.example.bigdata.tools.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class HouseStatsAnalysis {
    public static void main(String[] args) throws Exception {

        ParameterTool properties = Properties.get(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<ScoreEvent> scoreEventDS;

        if (properties.getRequired("data.input").equals("array")) {
            scoreEventDS = env
                    .addSource(new ScoreEventArraySource(Inputs.getJsonUnorderedStrings(),20))
                    .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
        } else {
            scoreEventDS = env
                    .fromSource(ScoreEventKafkaSource.create(properties),
                            WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        }

        DataStream<HouseStatsResult> houseStatsDS = scoreEventDS
                .keyBy(ScoreEvent::getHouse)
                .window(TumblingEventTimeWindows.of(Time.hours(6)))
                .aggregate(new HouseStatsAggregator(), new HouseStatsProcessWindowFunction());

        if (properties.getRequired("data.output").equals("console")) {
            houseStatsDS.print();
        } else {
            houseStatsDS.addSink(MySQLSink.create(properties, MySQLSink.INSERT_COMMAND));
        }

        env.execute("House Stats Analysis");
    }
}

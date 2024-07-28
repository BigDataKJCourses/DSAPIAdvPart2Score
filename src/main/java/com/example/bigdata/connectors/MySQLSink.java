package com.example.bigdata.connectors;

import com.example.bigdata.model.HouseStatsResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLSink {
    public static final String INSERT_COMMAND = "insert into house_stats_sink " +
            "(how_many, sum_score, characters, " +
            "house, start_date, end_date) \n" +
            "values (?, ?, ?, ?, ?, ?)";
    public static final String UPDATE_COMMAND = "update house_stats_sink " +
            "set how_many = ?, sum_score = ?, characters = ? \n" +
            "where house = ? and start_date = ? and end_date = ? ";

    public static final String UPSERT_COMMAND = "insert into house_stats_sink " +
            "(how_many, sum_score, characters, " +
            "house, start_date, end_date) \n" +
            "values (?, ?, ?, ?, ?, ?) \n" +
            "ON DUPLICATE KEY UPDATE\n" +
            "  how_many = ?, " +
            "  sum_score = ?, " +
            "  characters = ?;";

    public static SinkFunction<HouseStatsResult> create(ParameterTool properties, String command) {
        String url = properties.getRequired("mysql.url");
        String username = properties.getRequired("mysql.username");
        String password = properties.getRequired("mysql.password");

        JdbcStatementBuilder<HouseStatsResult> statementBuilder =
                new JdbcStatementBuilder<HouseStatsResult>() {
                    @Override
                    public void accept(PreparedStatement ps, HouseStatsResult data) throws SQLException {
                        ps.setLong(1, data.getHowMany());
                        ps.setLong(2, data.getSumScore());
                        ps.setLong(3, data.getNoCharacters());
                        ps.setString(4, data.getHouse());
                        ps.setLong(5, data.getFrom());
                        ps.setLong(6, data.getTo());
                        if (command.equals(UPSERT_COMMAND)) {
                            ps.setLong(7, data.getHowMany());
                            ps.setLong(8, data.getSumScore());
                            ps.setLong(9, data.getNoCharacters());
                        }
                    }
                };

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        SinkFunction<HouseStatsResult> jdbcSink =
                JdbcSink.sink(command,
                        statementBuilder,
                        executionOptions,
                        connectionOptions);

        return jdbcSink;
    }
}

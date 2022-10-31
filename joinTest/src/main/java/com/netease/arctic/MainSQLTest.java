package com.netease.arctic;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MainSQLTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setCheckpointInterval(60000L);
        checkpointConfig.setCheckpointTimeout(1800000L);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.dynamic-table-options.enabled", "true");
        configuration.setString("execution.checkpointing.unaligned.forced", "true");
        tableEnv.executeSql("Create catalog c WITH ('type'='arctic', " +
            "'metastore.url'='thrift://10.196.98.23:18112/trino_online_env')");
        String s1 = "create table orders (\n    " +
            "o_w_id int, o_d_id int, o_id int,\n    " +
            "ts as localtimestamp, watermark for ts as ts\n) " +
            "with ('connector'='datagen', 'rows-per-second'='1', " +
            "'fields.o_w_id.max'='15800000','fields.o_w_id.min'='1')";
        tableEnv.executeSql(s1);
        String s2 = "create table if not exists user_dim  (\n" +
            "optime TIMESTAMP(3),\n" +
            "WATERMARK FOR optime AS optime \n" +
            ") LIKE c.upsertSpeedTest.hugeTestTable";
        tableEnv.executeSql(s2);
        String s3 = "CREATE TABLE p (o_w_id int, o_d_id int, o_id int, c_w_id int, c_id int, c_credit string) " +
            "WITH ('connector'='blackhole')";
        tableEnv.executeSql(s3);
        String s4 = "SELECT o.o_w_id,o.o_d_id,o.o_id, user_dim.c_w_id, user_dim.c_id, user_dim.c_credit \n" +
            "FROM orders o \n" +
            "LEFT JOIN user_dim /*+OPTIONS('streaming'='true','arctic.read.mode'='file'," +
            "'scan.startup.mode'='earliest', 'dim-table.enable'='true')*/ \n" +
            "FOR SYSTEM_TIME AS OF o.ts \n" +
            "ON o.o_w_id = user_dim.c_w_id and o.o_d_id=user_dim.c_d_id and o.o_id=user_dim.c_id";
//        tableEnv.sqlQuery(s4).execute().print();
        DataStream<Row> ds =tableEnv.toChangelogStream(tableEnv.sqlQuery(s4));
        ds.addSink(new DiscardingSink<>()).disableChaining();
        env.executeAsync("test");
    }
}
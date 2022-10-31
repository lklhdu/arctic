package com.netease.arctic;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.types.Row;

public class readTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        String s1 = "create table if not exists user_dim  (\n" +
            "optime TIMESTAMP(3),\n" +
            "WATERMARK FOR optime AS optime \n" +
            ") LIKE c.upsertSpeedTest.hugeTestTable\n";
        tableEnv.executeSql(s1);
        String s2 = "SELECT c_d_id FROM user_dim /*+OPTIONS('streaming'='true','arctic.read.mode'='file','scan.startup.mode'='earliest')*/";
//        DataStream<Row> ds =tableEnv.toChangelogStream(tableEnv.sqlQuery(s2));
//        ds.addSink(new DiscardingSink<>()).disableChaining();
        tableEnv.executeSql(s2);
    }
}

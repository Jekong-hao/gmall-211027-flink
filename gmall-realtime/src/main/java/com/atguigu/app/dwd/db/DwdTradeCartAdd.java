package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradeCartAdd {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //env.setStateBackend(new HashMapStateBackend());
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");

        //设置状态存储时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //TODO 2.使用DDL方式读取Kafka topic_db 主题数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_cart_add_211027"));

        //打印测试
        Table table = tableEnv.sqlQuery("select * from topic_db");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        rowDataStream.print(">>>>>>>>>>>");

        //TODO 3.过滤出加购数据

        //TODO 4.读取MySQL中的base_dic表构建维表

        //TODO 5.关联两张表   维度退化

        //TODO 6.将数据写回到Kafka DWD层

        //TODO 7.启动任务
        env.execute("DwdTradeCartAdd");

    }

}

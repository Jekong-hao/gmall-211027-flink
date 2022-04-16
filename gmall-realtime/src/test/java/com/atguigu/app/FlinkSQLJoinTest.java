package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkSQLJoinTest {

    public static void main(String[] args) {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //读取数据创建流
        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean1(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean2(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        //将流转换为动态表
        tableEnv.createTemporaryView("t1", bean1DS);
        tableEnv.createTemporaryView("t2", bean2DS);

        //内连接       左表：OnCreateAndWrite   右表：OnCreateAndWrite
//        tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id = t2.id")
//                .execute()
//                .print();

        //左外连接     左表：OnReadAndWrite     右表：OnCreateAndWrite
//        tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id = t2.id")
//                .execute()
//                .print();

        //右外连接     左表：OnCreateAndWrite   右表：OnReadAndWrite
//        tableEnv.sqlQuery("select t2.id,t1.name,t2.sex from t1 right join t2 on t1.id = t2.id")
//                .execute()
//                .print();

        //全外连接     左表：OnReadAndWrite     右表：OnReadAndWrite
        tableEnv.sqlQuery("select t1.id,t2.id,t1.name,t2.sex from t1 full join t2 on t1.id = t2.id")
                .execute()
                .print();

    }

}

package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.OrderDetailFilterFunction;
import com.atguigu.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.atguigu.utils.DimUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsTradeTrademarkCategoryUserSpuOrderWindow {

    public static void main(String[] args) {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.获取过滤后的OrderDetail表
        String groupId = "sku_user_order_window_211027";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);

        //TODO 3.转换数据为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> skuUserOrderDS = orderDetailJsonObjDS.map(json -> TradeTrademarkCategoryUserSpuOrderBean.builder()
                .skuId(json.getString("sku_id"))
                .userId(json.getString("user_id"))
                .orderCount(1L)
                .orderAmount(json.getDouble("split_total_amount"))
                .build()
        );

        //TODO 4.关联维表
        skuUserOrderDS.map(new RichMapFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建连接
            }

            @Override
            public TradeTrademarkCategoryUserSpuOrderBean map(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {

                //查询SKU表
                //DimUtil.getDimInfo(conn, "", value.getSkuId());

                //查询SPU表

                //... ...

                return value;
            }
        });

        //TODO 5.提取时间戳生成WaterMark

        //TODO 6.分组、开窗聚合

        //TODO 7.将数据写出到ClickHouse

        //TODO 8.启动任务

    }

}

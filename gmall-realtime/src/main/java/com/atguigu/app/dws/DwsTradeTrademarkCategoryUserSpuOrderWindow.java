package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.app.func.OrderDetailFilterFunction;
import com.atguigu.bean.TradeTrademarkCategoryUserSpuOrderBean;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class DwsTradeTrademarkCategoryUserSpuOrderWindow {

    public static void main(String[] args) throws Exception {

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
//        skuUserOrderDS.map(new RichMapFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean>() {
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                //创建连接
//            }
//            @Override
//            public TradeTrademarkCategoryUserSpuOrderBean map(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {
//                //查询SKU表
//                //DimUtil.getDimInfo(conn, "", value.getSkuId());
//                //查询SPU表
//                //... ...
//                return value;
//            }
//        });
        skuUserOrderDS.print("skuUserOrderDS>>>>");

        //4.1 关联SKU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSkuDS = AsyncDataStream.unorderedWait(
                skuUserOrderDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            System.out.println("CATEGORY3_ID:" + dimInfo.getString("CATEGORY3_ID"));
                            input.setSpuId(dimInfo.getString("SPU_ID"));
                            input.setTrademarkId(dimInfo.getString("TM_ID"));
                            input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        //4.2 关联SPU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSpuDS = AsyncDataStream.unorderedWait(
                withSkuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getSpuId();
                    }
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setSpuName(dimInfo.getString("SPU_NAME"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        //4.3 关联TM
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setTrademarkName(dimInfo.getString("TM_NAME"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        //4.4 关联Category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory3DS = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        System.out.println("input.getCategory3Id():" + input.getCategory3Id());
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory3Name(dimInfo.getString("NAME"));
                            input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        //4.5 关联Category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory2DS = AsyncDataStream.unorderedWait(
                withCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory2Name(dimInfo.getString("NAME"));
                            input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        //4.6 关联Category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1DS = AsyncDataStream.unorderedWait(
                withCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory1Name(dimInfo.getString("NAME"));
                        }
                    }
                },
                60, TimeUnit.SECONDS);

        //打印测试
        withCategory1DS.print("withCategory1DS>>>>>>>>");

        //TODO 5.提取时间戳生成WaterMark

        //TODO 6.分组、开窗聚合

        //TODO 7.将数据写出到ClickHouse

        //TODO 8.启动任务
        env.execute("DwsTradeTrademarkCategoryUserSpuOrderWindow");

    }

}

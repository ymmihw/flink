package com.ymmihw.bigdata.flink;

import java.sql.Timestamp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import com.ymmihw.bigdata.flink.assigner.MyTimestampAssigner;
import com.ymmihw.bigdata.flink.assigner.MyWatermarkGenerator;
import com.ymmihw.bigdata.flink.source.PageViewsSource;

public class StreamWindowingExample {

  public static void doStreamWindowing(StreamExecutionEnvironment env,
      StreamTableEnvironment tableEnv) {
    WatermarkStrategy<Tuple2<String, Long>> watermarkStrategy =
        WatermarkStrategy.forGenerator(c -> new MyWatermarkGenerator())
            .withTimestampAssigner(e -> new MyTimestampAssigner());
    DataStream<Tuple2<String, Long>> pageViews =
        env.addSource(new PageViewsSource(1000)).assignTimestampsAndWatermarks(watermarkStrategy);

    tableEnv.createTemporaryView("PageViews", pageViews,
        ExpressionParser.parseExpressionList("nation, rowtime.rowtime").toArray(new Expression[0]));
    Table resultHop = tableEnv.sqlQuery(
        "SELECT nation , COUNT(*) FROM PageViews GROUP BY HOP( rowtime , INTERVAL '10' SECOND, INTERVAL '10' SECOND) , nation");
    DataStream<Tuple2<String, Long>> queryResultHop = tableEnv.toAppendStream(resultHop,
        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));
    queryResultHop.print();


    Table resultSession = tableEnv.sqlQuery(
        "SELECT nation , COUNT(*), SESSION_START(rowtime , INTERVAL '1' SECOND), SESSION_ROWTIME(rowtime , INTERVAL '1' SECOND)  FROM PageViews GROUP BY SESSION( rowtime , INTERVAL '1' SECOND) , nation");
    DataStream<Tuple4<String, Long, Timestamp, Timestamp>> queryResultSession =
        tableEnv.toAppendStream(resultSession,
            TypeInformation.of(new TypeHint<Tuple4<String, Long, Timestamp, Timestamp>>() {}));
    queryResultSession.print();
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    doStreamWindowing(env, tableEnv);

    env.execute("Example SQL");
  }
}

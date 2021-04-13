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
    // Stream windowing
    // Assume a source of page views that reports the country from which the page view is coming and
    // time stamp of the view
    // In this example, we first register the stream of page views as a table with two columns
    // nation and rowtime, to refer to the timestamp
    DataStream<Tuple2<String, Long>> pageViews =
        env.addSource(new PageViewsSource(1000)).assignTimestampsAndWatermarks(
            WatermarkStrategy.forGenerator(c -> new MyWatermarkGenerator())
                .withTimestampAssigner(e -> new MyTimestampAssigner()));

    tableEnv.createTemporaryView("PageViews", pageViews,
        ExpressionParser.parseExpressionList("nation, rowtime.rowtime").toArray(new Expression[0]));
    // We apply a hopping (sliding) window over the table
    // It is like an aggregate query where to group by nation column and the hopping window
    // specification of 1 minute window that updates every second
    Table resultHop = tableEnv.sqlQuery(
        "SELECT nation , COUNT(*) FROM PageViews GROUP BY HOP( rowtime , INTERVAL '10' SECOND, INTERVAL '10' SECOND) , nation");
    // Here, we apply a session window on the same original stream (table) of page views. The
    // session timeout is 1 second
    // We use the Session_start function to project the session start time stamp as a column in the
    // results
    Table resultSession = tableEnv.sqlQuery(
        "SELECT nation , COUNT(*), SESSION_START(rowtime , INTERVAL '1' SECOND), SESSION_ROWTIME(rowtime , INTERVAL '1' SECOND)  FROM PageViews GROUP BY SESSION( rowtime , INTERVAL '1' SECOND) , nation");

    // We need to transform the tuples in the tables carrying query results back to streams.
    // In CQL R2S (Relation to stream) transformation.
    // In order to do so, we need to let the stream know about the schema of the tuples
    // TypeInformation<Tuple2<String, Long>> tupleTypeHop =
    // new TupleTypeInfo<>(DataTypes.STRING(), DataTypes.BIGINT());

    // TupleTypeInfo<Tuple4<String, Long, Long, Long>> tupleTypeSession = new TupleTypeInfo<>(
    // DataTypes.STRING(), DataTypes.LONG(), DataTypes.SQL_TIMESTAMP(), DataTypes.SQL_TIMESTAMP());

    // Write results from the table back to an append-only stream
    DataStream<Tuple2<String, Long>> queryResultHop = tableEnv.toAppendStream(resultHop,
        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));
    // Prints a tuple of 2 of <String, Long> which is the country name and the count of views in the
    // respective window instance.
    queryResultHop.print();


    DataStream<Tuple4<String, Long, Timestamp, Timestamp>> queryResultSession =
        tableEnv.toAppendStream(resultSession,
            TypeInformation.of(new TypeHint<Tuple4<String, Long, Timestamp, Timestamp>>() {}));
    // Prints a tuple of 4 <String, Long, DateTime, DateTime> which corresponds to nation, number of
    // views, session window start,
    // inclusive session window end
    queryResultSession.print();
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    doStreamWindowing(env, tableEnv);

    env.execute("Example SQL");
  }
}

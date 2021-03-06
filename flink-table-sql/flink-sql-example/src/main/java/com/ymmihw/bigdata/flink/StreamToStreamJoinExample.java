package com.ymmihw.bigdata.flink;

import java.sql.Timestamp;
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
import com.ymmihw.bigdata.flink.source.ImpressionSource;

public class StreamToStreamJoinExample {


  public static void doStreamToStreamJoin(StreamExecutionEnvironment env,
      StreamTableEnvironment tableEnv) {
    DataStream<Tuple2<String, Long>> impressions = env.addSource(new ImpressionSource(500));
    DataStream<Tuple2<String, Long>> clicks = env.addSource(new ImpressionSource(100));
    tableEnv.createTemporaryView("Impressions", impressions, ExpressionParser
        .parseExpressionList("impressionID, rowtime.rowtime").toArray(new Expression[0]));
    tableEnv.createTemporaryView("Clicks", clicks, ExpressionParser
        .parseExpressionList("clickID, rowtime.rowtime").toArray(new Expression[0]));
    Table streamStreamJoinResult = tableEnv.sqlQuery(
        "SELECT i.impressionID, i.rowtime as impresstionTime, c.clickID, cast (c.rowtime as TIMESTAMP) as clickTime FROM Impressions i , Clicks c WHERE i.impressionID = c.clickID AND c.rowtime BETWEEN i.rowtime - INTERVAL '1' SECOND AND i.rowtime");
    DataStream<Tuple4<String, Timestamp, String, Timestamp>> streamStreamJoinResultAsStream =
        tableEnv.toAppendStream(streamStreamJoinResult,
            TypeInformation.of(new TypeHint<Tuple4<String, Timestamp, String, Timestamp>>() {}));

    streamStreamJoinResultAsStream.print();
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    doStreamToStreamJoin(env, tableEnv);
    env.execute("Example SQL");
  }
}

package com.ymmihw.bigdata.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import com.ymmihw.bigdata.flink.source.SensorSource;

public class StreamTableJoinExample {
  public static void doStreamTableJoin(StreamExecutionEnvironment env,
      StreamTableEnvironment tableEnv) {
    DataStream<Tuple4<String, String, Double, Long>> sensorReadings =
        env.addSource(new SensorSource());
    tableEnv.createTemporaryView("SensorReadings", sensorReadings,
        ExpressionParser.parseExpressionList("sensorID, lineID, readingValue, rowtime.rowtime")
            .toArray(new Expression[0]));
    CsvTableSource itemsSource = new CsvTableSource.Builder().path("data.csv").ignoreParseErrors()
        .field("ItemID", DataTypes.STRING()).field("LineID", DataTypes.STRING()).build();

    DataStream<Row> dataSet = itemsSource.getDataStream(env);
    tableEnv.createTemporaryView("Items", dataSet);

    Table streamTableJoinResult = tableEnv.sqlQuery(
        "SELECT S.sensorID , S.readingValue , I.ItemID FROM SensorReadings S LEFT JOIN Items I ON S.lineID = I.LineID");

    DataStream<Tuple2<Boolean, Row>> joinQueryResult =
        tableEnv.toRetractStream(streamTableJoinResult, Row.class);


    joinQueryResult.print();

  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    doStreamTableJoin(env, tableEnv);
    env.execute("Example SQL");
  }
}

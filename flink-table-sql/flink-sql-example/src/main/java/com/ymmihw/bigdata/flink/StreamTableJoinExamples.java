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

public class StreamTableJoinExamples {
  public static void doStreamTableJoin(StreamExecutionEnvironment env,
      StreamTableEnvironment tableEnv) {
    // Stream to table joins
    // Assume readings from a sensor that report the sensor id, line ID, reading value and the time
    // stamp
    DataStream<Tuple4<String, String, Double, Long>> sensorReadings =
        env.addSource(new SensorSource());
    // Register as a table in order to run SQL queries against it.
    tableEnv.createTemporaryView("SensorReadings", sensorReadings,
        ExpressionParser.parseExpressionList("sensorID, lineID, readingValue, rowtime.rowtime")
            .toArray(new Expression[0]));

    // This is a finite list of items which has an item ID an a line ID.
    CsvTableSource itemsSource = new CsvTableSource.Builder().path("data.csv").ignoreParseErrors()
        .field("ItemID", DataTypes.STRING()).field("LineID", DataTypes.STRING()).build();

    DataStream<Row> dataSet = itemsSource.getDataStream(env);
    tableEnv.createTemporaryView("Items", dataSet);

    // Similarly, register this list of items as a table.
    // tableEnv.registerTableSource("Items", itemsSource);


    // Apply a join query and store the results in a dynamic (stream table) table
    Table streamTableJoinResult = tableEnv.sqlQuery(
        "SELECT S.sensorID , S.readingValue , I.ItemID FROM SensorReadings S LEFT JOIN Items I ON S.lineID = I.LineID");

    // Transform the results again to a stream so that we can print it or use it elsewhere.
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

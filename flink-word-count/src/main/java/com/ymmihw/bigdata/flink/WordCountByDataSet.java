package com.ymmihw.bigdata.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCountByDataSet {
  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // @formatter:off
    DataSet<String> text = env.fromElements(
        "To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,");
    // @formatter:on

    DataSet<Tuple2<String, Integer>> counts =
        text.flatMap(new LineSplitter()).groupBy(0).aggregate(Aggregations.SUM, 1);

    // emit result
    counts.print();
  }
}

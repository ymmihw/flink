package com.ymmihw.bigdata.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.util.Collector;

public class WordCountByTable {
  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

    // @formatter:off
    DataSource<String> text = env.fromElements(
        "To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,");
    // @formatter:on

    // split the sentences into words
    FlatMapOperator<String, String> dataset =
        text.flatMap((String value, Collector<String> out) -> {
          for (String token : value.toLowerCase().split("\\W+")) {
            if (token.length() > 0) {
              out.collect(token);
            }
          }
        }).returns(String.class);

    // create a table named "words" from the dataset
    tableEnv.createTemporaryView("words", dataset, ExpressionParser.parseExpression("word"));

    // word count using an sql query
    TableResult results = tableEnv.executeSql("select word, count(*) from words group by word");

    results.print();

  }
}



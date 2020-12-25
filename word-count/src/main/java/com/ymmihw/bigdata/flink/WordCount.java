package com.ymmihw.bigdata.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class WordCount {
  public static void main(String[] args) throws Exception {
    // set up the execution environment
    final ParameterTool params = ParameterTool.fromArgs(args);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params);
    // get input data
    DataSet<String> text = env.readTextFile(params.get("input", "input.txt"));
    DataSet<Tuple2<String, Integer>> counts =
        // split up the lines in pairs (2-tuples) containing: (word,1)
        text.flatMap(new Splitter())
            // group by the tuple field "0" and sum up tuple field "1"
            .groupBy(0).aggregate(Aggregations.SUM, 1);
    // emit result
//    counts.writeAsText(params.get("output", "output"));
    counts.print();
    // execute program
    env.execute("WordCount Example");
  }
}


// The operations are defined by specialized classes, here the Splitter class.
class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
  private static final long serialVersionUID = 1L;

  @Override
  public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
    // normalize and split the line into words
    String[] tokens = value.split("\\W+");
    // emit the pairs
    for (String token : tokens) {
      if (token.length() > 0) {
        out.collect(new Tuple2<String, Integer>(token, 1));
      }
    }
  }
}

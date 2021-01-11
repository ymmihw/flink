package com.ymmihw.bigdata.flink;

import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountByDataStream {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // @formatter:off
    DataStreamSource<String> text = env.fromElements(
        "To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles,");
    // @formatter:on

    text.flatMap(new LineSplitter())
        .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class)).keyBy(x -> x.f0)
        .sum(1).print();

    env.execute();
  }
}



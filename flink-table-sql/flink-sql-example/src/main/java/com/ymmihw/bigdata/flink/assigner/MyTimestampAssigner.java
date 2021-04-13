package com.ymmihw.bigdata.flink.assigner;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyTimestampAssigner implements TimestampAssigner<Tuple2<String, Long>> {

  @Override
  public long extractTimestamp(Tuple2<String, Long> item, long l) {
    return item.f1;
  }
}

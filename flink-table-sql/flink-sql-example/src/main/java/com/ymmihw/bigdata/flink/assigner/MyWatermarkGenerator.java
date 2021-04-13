package com.ymmihw.bigdata.flink.assigner;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>> {

  private long maxTimestamp = 0;

  @Override
  public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
    maxTimestamp = Math.max(maxTimestamp, event.f1);
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput output) {
    output.emitWatermark(new Watermark(maxTimestamp));
  }
}

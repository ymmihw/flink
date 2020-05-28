package com.ymmihw.bigdata.flink.operator;

import java.time.ZoneId;
import javax.annotation.Nullable;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import com.ymmihw.bigdata.flink.model.InputMessage;

public class InputMessageTimestampAssigner
    implements AssignerWithPunctuatedWatermarks<InputMessage> {
  private static final long serialVersionUID = 1L;

  @Override
  public long extractTimestamp(InputMessage element, long previousElementTimestamp) {
    ZoneId zoneId = ZoneId.systemDefault();
    return element.getSentAt().atZone(zoneId).toEpochSecond() * 1000;
  }

  @Nullable
  @Override
  public Watermark checkAndGetNextWatermark(InputMessage lastElement, long extractedTimestamp) {
    return new Watermark(extractedTimestamp - 15);
  }
}

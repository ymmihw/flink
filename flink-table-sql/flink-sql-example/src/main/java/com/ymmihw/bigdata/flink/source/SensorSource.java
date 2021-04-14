package com.ymmihw.bigdata.flink.source;


import java.util.Random;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SensorSource implements SourceFunction<Tuple4<String, String, Double, Long>> {
  private static final long serialVersionUID = 1L;
  private static final int numSources = 3;
  private boolean canceled = false;
  private Random random;

  public SensorSource() {
    random = new Random(5);
  }

  @Override
  public void run(SourceContext<Tuple4<String, String, Double, Long>> sourceContext)
      throws Exception {
    int currentSource = 0;
    while (!canceled) {
      long ts = System.currentTimeMillis();
      Tuple4<String, String, Double, Long> data = new Tuple4<>(String.valueOf(currentSource),
          String.valueOf(currentSource), random.nextDouble(), ts);
      sourceContext.collectWithTimestamp(data, ts);
      currentSource = (currentSource + 1) % numSources;
      long sleepTime = 10;
      Thread.sleep(sleepTime);
    }
  }

  @Override
  public void cancel() {
    canceled = true;
  }
}

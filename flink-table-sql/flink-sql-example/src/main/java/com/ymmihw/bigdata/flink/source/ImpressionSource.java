package com.ymmihw.bigdata.flink.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class ImpressionSource implements SourceFunction<Tuple2<String, Long>> {
  private static final long serialVersionUID = 1L;
  private static final int numIDs = 3;
  private boolean canceled = false;
  private long delay = 1;
  private Random random;


  public ImpressionSource(long averageInterArrivalTime) {
    this.delay = averageInterArrivalTime;
    random = new Random();
  }

  @Override
  public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
    int currentID = 0;
    while (!canceled) {
      long ts = System.currentTimeMillis();
      sourceContext.collectWithTimestamp(new Tuple2<>(String.valueOf(currentID), ts), ts);
      currentID = (currentID + 1) % numIDs;
      long sleepTime = (long) (random.nextGaussian() * 0.5D + delay);
      Thread.sleep(sleepTime);
    }
  }

  @Override
  public void cancel() {
    canceled = true;

  }

}

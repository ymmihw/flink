package com.ymmihw.bigdata.flink.source;

import java.util.Random;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class PageViewsSource implements SourceFunction<Tuple2<String, Long>> {
  private static final long serialVersionUID = 1L;
  private static final int numCountries = 3;
  private boolean canceled = false;
  private long delay = 1;
  private Random random;


  public PageViewsSource(long averageInterArrivalTime) {
    this.delay = averageInterArrivalTime;
    random = new Random();
  }

  @Override
  public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
    int currentCountry = 0;
    while (!canceled) {
      long ts = System.currentTimeMillis();
      sourceContext
          .collectWithTimestamp(new Tuple2<String, Long>(getCountryName(currentCountry), ts), ts);
      currentCountry = (currentCountry + 1) % numCountries;
      long sleepTime = (long) (random.nextGaussian() * 0.5D + delay);
      Thread.sleep(sleepTime);
    }
  }

  @Override
  public void cancel() {
    canceled = true;

  }

  private String getCountryName(int countryCode) {
    if (countryCode == 0)
      return "Estonia";
    else if (countryCode == 1)
      return "Italy";
    else if (countryCode == 2)
      return "Egypt";
    else
      return "NaC"; // not a country :)
  }
}

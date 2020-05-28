package com.ymmihw.bigdata.flink.operator;

import org.apache.flink.api.common.functions.MapFunction;

public class WordsCapitalizer implements MapFunction<String, String> {
  private static final long serialVersionUID = 1L;

  @Override
  public String map(String s) {
    return s.toUpperCase();
  }
}

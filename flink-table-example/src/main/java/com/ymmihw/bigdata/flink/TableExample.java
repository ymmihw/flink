package com.ymmihw.bigdata.flink;

import static org.apache.flink.table.expressions.ExpressionParser.parseExpression;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class TableExample {
  public static void main(String[] args) throws Exception {
    // create the environments
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

    // get the path to the file in resources folder
    String peoplesPath = TableExample.class.getClassLoader().getResource("peoples.csv").getPath();
    // load the csv into a table
    // register the table and scan it
    CsvTableSource tableSource =
        new CsvTableSource(peoplesPath, "id,last_name,country,gender".split(","),
            new TypeInformation[] {Types.INT, Types.STRING, Types.STRING, Types.STRING});
    DataSet<Row> dataSet = tableSource.getDataSet(env);

    tableEnv.createTemporaryView("peoples", dataSet);

    Table peoples = tableEnv.from("peoples");

    // aggregation using chain of methods
    Table countriesCount = peoples.groupBy(parseExpression("country"))
        .select(parseExpression("country"), parseExpression("id.count"));
    DataSet<Row> result1 = tableEnv.toDataSet(countriesCount, Row.class);
    result1.print();

    // aggregation using SQL syntax
    TableResult tableResult = tableEnv
        .executeSql("select country, gender, count(id) from peoples group by country, gender");

    tableResult.print();
  }
}

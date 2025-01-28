package yangkai.com.flinkjava.code.chapter2;

import java.util.Arrays;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink 批处理workdcount
 * @author bytedance
 */
public class BatchWordCount {

  private static void demo1() throws Exception {
    // 准备flin的执行环境
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    //1.读取文件
    DataSource<String> linesDS = env.readTextFile("./data/words.txt");
    //2.切分单词
    FlatMapOperator<String, String> wordsDS = linesDS.flatMap(
        new FlatMapFunction<String, String>() {
          @Override
          public void flatMap(String line, Collector<String> collector) throws Exception {
            String[] arr = line.split(" ");
            Arrays.stream(arr).forEach(collector::collect);
          }
        });
    //3.单词计数
    MapOperator<String, Tuple2<String, Long>> kvWordsDS = wordsDS.map(
            (MapFunction<String, Tuple2<String, Long>>) word -> Tuple2.of(word, 1L))
        .returns(Types.TUPLE(Types.STRING, Types.LONG));
    //4.按照单词分组并打印
    kvWordsDS.groupBy(0).sum(1).print();
  }

  private static void demo2() throws Exception {
    // 准备flin的执行环境
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    //1.读取文件
    DataSource<String> linesDS = env.readTextFile("./data/words.txt");
    FlatMapOperator<String, Tuple2<String, Long>> kvWordsDS = linesDS.flatMap(
        (FlatMapFunction<String, Tuple2<String, Long>>) (line, collector) -> {
          String[] arr = line.split(" ");
          Arrays.stream(arr).map(word -> Tuple2.of(word, 1L)).forEach(collector::collect);
        }
    ).returns(
        Types.TUPLE(Types.STRING, Types.LONG)
    );
    kvWordsDS.groupBy(0).sum(1).print();
  }


  public static void main(String[] args) throws Exception {
//    demo1();
    demo2();
  }

}

package yangkai.com.flinkjava.code.chapter2;

import java.util.Arrays;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author bytedance
 */
public class StreamBatchModeWordCount {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);  // 会根据数据源来自动调整模式

    DataStreamSource<String> linesDS = env.readTextFile("./data/words.txt");

    linesDS
        .flatMap((String line, Collector<String> collector) -> Arrays.stream(line.split(" "))
            .forEach(collector::collect))
        .returns(Types.STRING)
        .map(word -> Tuple2.of(word, 1L))
        .returns(Types.TUPLE(Types.STRING, Types.LONG))
        .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
          @Override
          public String getKey(Tuple2<String, Long> tp) throws Exception {
            return tp.f0;
          }
        })
        .sum(1)
        .print();

    // 流式计算中需要最后执行execute方法
    env.execute();
  }

}

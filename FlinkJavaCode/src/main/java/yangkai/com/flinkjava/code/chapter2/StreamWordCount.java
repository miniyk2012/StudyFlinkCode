package yangkai.com.flinkjava.code.chapter2;


import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 流处理workdcount
 * @author bytedance
 */
public class StreamWordCount {

  private static void demo1() throws Exception {
    //1.创建流式处理环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<String> linesDS = env.readTextFile("./data/words.txt");
    linesDS
        .flatMap((String line, Collector<String> collector) -> Arrays.stream(line.split(" ")).forEach(collector::collect))
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

  public static void main(String[] args) throws Exception {
    demo1();
  }
}

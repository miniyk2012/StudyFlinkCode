package yangkai.com.flinkjava.code.chapter3;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 读取socket数据, 实时统计wordcount
public class SocketWordCount {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // hello,flink
    DataStreamSource<String> ds = env.socketTextStream("localhost", 9999);

    SingleOutputStreamOperator<Tuple2<String, Integer>> tpDS = ds.flatMap(
        new FlatMapFunction<String, Tuple2<String, Integer>>() {
          @Override
          public void flatMap(String s, Collector<Tuple2<String, Integer>> collector)
              throws Exception {
            String[] arr = s.split(",");
            for (String word : arr) {
              collector.collect(Tuple2.of(word, 1));
            }
          }
        });
    tpDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
      @Override
      public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        return stringIntegerTuple2.f0;
      }
    }).sum(1).print();
    env.execute();
  }
}

/**
 * nc -lk 9999
 * hello,world
 * flink,java
 * flink,python,java
 * good,better
 * python,flink,java
 */
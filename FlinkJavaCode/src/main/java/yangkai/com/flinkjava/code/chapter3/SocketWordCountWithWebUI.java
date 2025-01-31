package yangkai.com.flinkjava.code.chapter3;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

// 本地运行, 查看webUI: 读取socket数据, 实时统计wordcount
public class SocketWordCountWithWebUI {

  public static void main(String[] args) throws Exception {
    // 1. 设置webUI运行在本地
    Configuration conf = new Configuration();
    conf.set(RestOptions.BIND_PORT, "8083");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
        conf);

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
package flinkscala.code.chapter3

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SocketWordCountWithWebUI {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set(RestOptions.BIND_PORT, "8083")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    import org.apache.flink.streaming.api.scala._

    val ds = env.socketTextStream("localhost", 9999)
    ds.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()
    env.execute()
  }
}

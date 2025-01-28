package flinkscala.code.chapter2

import org.apache.flink.api.scala.ExecutionEnvironment


/**
 * scala 批处理 word count
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 导入隐式转换
    import org.apache.flink.api.scala._
    val linesDS = env.readTextFile("./data/words.txt")


    linesDS.flatMap(_.split(" "))
      .map(word=>(word, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}

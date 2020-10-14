package ch.ethz.ml.graph.runner

import ch.ethz.ml.graph.data.{Delimiter, GraphIO}
import ch.ethz.ml.graph.trianglecount.{EdgeIterPlusUndirected, EdgeIterUndirected, FastUndirected, FastUndirectedV2, NodeIterPlusUndirected, NodeIterUndirected}
import ch.ethz.ml.graph.utils.ArgsUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object UndirectedRunner {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val algo = params.getOrElse("algo", "fast")
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", "data/gemsec-Facebook/artist_edges.csv")
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.SPACE))
    val output = params.getOrElse("output", "model/triangleCount")
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_AND_DISK"))
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val isWeighted = params.getOrElse("weighted", "false").toBoolean
    val isDirected = params.getOrElse("directed", "false").toBoolean
    val isHeader = params.getOrElse("header", "false").toBoolean
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))

    val version = System.getProperty("java.version")
    println(s"java version = $version")

    val spark = SparkSession
      .builder()
      .master(mode)
      .appName("TriangleCount")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setCheckpointDir(cpDir)
    sc.setLogLevel("WARN")

    val counter = algo match {
      case "fast" => new FastUndirected()
      case "fast_v2" => new FastUndirectedV2()
      case "node_iter" => new NodeIterUndirected()
      case "node_iter_plus" => new NodeIterPlusUndirected()
      case "edge_iter" => new EdgeIterUndirected()
      case "edge_iter_plus" => new EdgeIterPlusUndirected()
      case _ => throw new Exception(s"algo $algo not supported.")
    }

    counter.setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setIsDirected(isDirected)
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)

    val loadStart = System.currentTimeMillis()
    val df = GraphIO.load(input, isWeighted = isWeighted, isHeader = isHeader,
      srcIndex = srcIndex, dstIndex = dstIndex, sep = sep)
    df.show(1)
    val loadEnd = System.currentTimeMillis()
    println(s">>> load data cost ${loadEnd - loadStart} ms")

    val countStart = System.currentTimeMillis()
    val mapping = counter.transform(df)
    val countEnd = System.currentTimeMillis()
    println(s">>> triangle count cost ${countEnd - countStart} ms")

    val saveStart = System.currentTimeMillis()
    GraphIO.save(mapping, output)
    val saveEnd = System.currentTimeMillis()
    println(s">>> save results cost ${saveEnd - saveStart} ms")

    spark.stop()
  }
}


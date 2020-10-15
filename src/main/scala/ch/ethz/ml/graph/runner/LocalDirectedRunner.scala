package ch.ethz.ml.graph.runner

import ch.ethz.ml.graph.data.{Delimiter, GraphIO}
import ch.ethz.ml.graph.trianglecount.{EdgeIterDirected, EdgeIterPlusDirected, EdgeIterUndirected, FastDirected, FastDirectedV2, FastDirectedV3, FastUndirected, NodeIterDirected, NodeIterPlusDirected, NodeIterUndirected}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object LocalDirectedRunner {

  def main(args: Array[String]): Unit = {
    val algo = "fast_v3"
    val mode = "local"
    //val input = "data/wiki-Vote/Wiki-Vote.txt"
    val input = "data/test/directed.txt"
    val sep = Delimiter.parse(Delimiter.TAB)
    val output = "model/triangleCount"
    val partitionNum = 5
    val storageLevel = StorageLevel.fromString("MEMORY_ONLY")
    val srcIndex = 0
    val dstIndex = 1
    val isWeighted = false
    val isDirected = false
    val isHeader = true
    val cpDir = "cp"

    val spark = SparkSession
      .builder()
      .master(mode)
      .appName("TriangleCount")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setCheckpointDir(cpDir)
    sc.setLogLevel("WARN")

    val counter = algo match {
      case "fast" => new FastDirected()
      case "fast_v2" => new FastDirectedV2()
      case "fast_v3" => new FastDirectedV3()
      case "node_iter" => new NodeIterDirected()
      case "node_iter_plus" => new NodeIterPlusDirected()
      case "edge_iter" => new EdgeIterDirected()
      case "edge_iter_plus" => new EdgeIterPlusDirected()
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
    println(s">>> triangle count totally cost ${countEnd - countStart} ms")

    val saveStart = System.currentTimeMillis()
    GraphIO.save(mapping, output)
    val saveEnd = System.currentTimeMillis()
    println(s"save results cost ${saveEnd - saveStart} ms")

    spark.stop()
  }
}

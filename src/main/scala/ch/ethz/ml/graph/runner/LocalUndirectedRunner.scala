package ch.ethz.ml.graph.runner

import ch.ethz.ml.graph.data.{Delimiter, GraphIO}
import ch.ethz.ml.graph.trianglecount.{EdgeIterPlusUndirected, EdgeIterUndirected, FastUndirected, FastUndirectedV2, FastUndirectedV3, FastUndirectedV4, NodeIterPlusUndirected, NodeIterUndirected}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object LocalUndirectedRunner {

  def main(args: Array[String]): Unit = {

    val algo = "fast_v4"
    val mode = "local"
    //val input = "data/gemsec-Facebook/artist_edges.csv"
    val input = "data/test/undirected.txt"
    val sep = Delimiter.parse(Delimiter.COMMA)
    val output = "model/triangleCount"
    val partitionNum = 2
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
      case "fast" => new FastUndirected()
      case "fast_v2" => new FastUndirectedV2()
      case "fast_v3" => new FastUndirectedV3()
      case "fast_v4" => new FastUndirectedV4()
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
    println(s">>> triangle count totally cost ${countEnd - countStart} ms")

    val saveStart = System.currentTimeMillis()
    GraphIO.save(mapping, output)
    val saveEnd = System.currentTimeMillis()
    println(s">>> save results cost ${saveEnd - saveStart} ms")

    spark.stop()
  }
}

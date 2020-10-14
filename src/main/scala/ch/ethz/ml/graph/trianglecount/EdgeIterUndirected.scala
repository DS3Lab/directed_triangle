package ch.ethz.ml.graph.trianglecount

import ch.ethz.ml.graph.data.VertexId
import ch.ethz.ml.graph.params.{HasDstNodeIdCol, HasInput, HasIsDirected, HasPartitionNum, HasSrcNodeIdCol, HasStorageLevel}
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory


class EdgeIterUndirected(override val uid: String) extends Transformer
  with HasInput with HasSrcNodeIdCol with HasDstNodeIdCol with HasIsDirected
  with HasPartitionNum with HasStorageLevel {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def this() = this(Identifiable.randomUID("TriangleCount"))

  setDefault(srcNodeIdCol, "src")
  setDefault(dstNodeIdCol, "dst")

  override def transform(dataset: Dataset[_]): DataFrame = {
    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    val partNum = $(partitionNum)

    println(">>> generate neighbors from the dataset")
    val neighborStart = System.currentTimeMillis()
    val neighbors = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.flatMap { row =>
      Iterator((row.getLong(0), row.getLong(1)), (row.getLong(1), row.getLong(0)))
    }.groupByKey(partNum)
      .map { case (v, neighbor) =>
        (v, neighbor.filter(_ != v).toArray.distinct.sortWith(_ < _))
      }.persist($(storageLevel))
    println(s"count of neighbor tables = ${neighbors.count()}, number of partitions = ${neighbors.getNumPartitions}")
    val neighborEnd = System.currentTimeMillis()
    println(s"generate neighbors cost ${neighborEnd - neighborStart} ms")

    println(">>> load edges from neighbor tables")
    val edgeStart = System.currentTimeMillis()
    val edge = neighbors.flatMap { case (v, nb) =>
      nb.flatMap { nid =>
        if (nid > v) {
          val pid = EdgePartition2D.getPartition(v, nid, partNum)
          Iterator.single((pid, (v, nid)))
        } else
          Iterator.empty
      }
    }.partitionBy(new HashPartitioner(partNum))
      .map { case (_, (src, dst)) =>
        Edge(src, dst, null.asInstanceOf[Byte])
      }.persist($(storageLevel))
//    val edge = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.map { row =>
//      val pid = EdgePartition2D.getPartition(row.getLong(0), row.getLong(1), partNum)
//      (pid, (row.getLong(0), row.getLong(1)))
//    }.partitionBy(new HashPartitioner(partNum))
//      .map { case (_, (src, dst)) => Edge(src, dst, null.asInstanceOf[Byte]) }
//      .persist($(storageLevel))
    println(s"count of edge = ${edge.count()}, number of partitions = ${edge.getNumPartitions}")
    val edgeSize = edge.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of edge: max=${edgeSize.max}, min=${edgeSize.min}")
    val edgeEnd = System.currentTimeMillis()
    println(s"generate edges cost ${edgeEnd - edgeStart} ms")

    println(">>> checkpoint neighbor tables and edges")
    val cpStart = System.currentTimeMillis()
    neighbors.foreachPartition(_ => Unit)
    neighbors.checkpoint()
    edge.foreachPartition(_ => Unit)
    edge.checkpoint()
    val cpEnd = System.currentTimeMillis()
    println(s"checkpoint cost ${cpEnd - cpStart} ms")

    println(">>> count triangles")
    val countStart = System.currentTimeMillis()
    val graph = Graph(neighbors, edge)
    val totalClosedTriangle = EdgeIterUndirected.computeNumOfClosedTriangle(graph, $(storageLevel)) / 3.0
    println(s"numTriangle=$totalClosedTriangle")
    val countEnd = System.currentTimeMillis()
    println(s"count triangles cost ${countEnd - countStart} ms")

    val output = dataset
      .sparkSession
      .sparkContext
      .parallelize(Seq(totalClosedTriangle)).map { value =>
      Row(value)
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(output, outputSchema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("TriangleNum", DoubleType, nullable = false)
    ))
  }
}

object EdgeIterUndirected {

  def computeNumOfClosedTriangle(graphWithAdj: Graph[Array[VertexId], Byte],
                                 storageLevel: StorageLevel): Double = {
    graphWithAdj.triplets.mapPartitionsWithIndex { case (partId, iter) =>
      var count = 0
      var numTriplets = 0
      iter.foreach { edgeTriplet =>
        if (null != edgeTriplet.srcAttr && null != edgeTriplet.dstAttr) {
          var i = 0
          var j = 0
          while (i < edgeTriplet.srcAttr.length && j < edgeTriplet.dstAttr.length) {
            if (edgeTriplet.srcAttr(i) == edgeTriplet.dstAttr(j)) {
              count += 1
              i += 1
              j += 1
            } else if (edgeTriplet.srcAttr(i) > edgeTriplet.dstAttr(j)) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        numTriplets = numTriplets + 1
        if (numTriplets % 100000 == 0) println(s">>> $numTriplets triplets processed in partition $partId")
      }
      println(s">>> triangle counts in partition $partId = $count")
      Iterator.single(count)
    }.sum()
  }

  def computeNumOfTriangleCandidates(degrees: RDD[Int]): Double = {
    degrees.map(d => d * (d - 1) / 2).sum()
  }
}

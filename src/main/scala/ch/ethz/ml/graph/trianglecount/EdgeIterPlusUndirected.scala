package ch.ethz.ml.graph.trianglecount

import ch.ethz.ml.graph.data.{TriangleType, VertexId}
import ch.ethz.ml.graph.params._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

class EdgeIterPlusUndirected(override val uid: String) extends Transformer
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
      }
    println(s"count of neighbor tables = ${neighbors.count()}, number of partitions = ${neighbors.getNumPartitions}")
    val neighborEnd = System.currentTimeMillis()
    println(s"generate neighbors cost ${neighborEnd - neighborStart} ms")

    println(">>> generate vertex degrees")
    val degreeStart = System.currentTimeMillis()
    val degrees = neighbors.map { case (v, neighbors) =>
      (v, neighbors.length) }
    val maxDegree = degrees.map(_._2).max()
    println(s"max degree = $maxDegree")
    val bcDegrees = sc.broadcast(degrees.collectAsMap())
    val degreeEnd = System.currentTimeMillis()
    println(s"generate degrees cost ${degreeEnd - degreeStart} ms")

    println(">>> filter neighbors with degree")
    val filterStart = System.currentTimeMillis()
    val filterNeighbors = neighbors.map{ case (v, neighbors) =>
      (v, EdgeIterPlusUndirected.filterNeighborTable(v, neighbors, bcDegrees.value))
    }.persist($(storageLevel))
    println(s"count of filter neighbors = ${filterNeighbors.count()}, number of partitions = ${filterNeighbors.getNumPartitions}")
    val filterNeighborsSize = filterNeighbors.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of filtered neighbor tables: max=${filterNeighborsSize.max}, min=${filterNeighborsSize.min}")
    val filterEnd = System.currentTimeMillis()
    println(s"filter neighbors cost ${filterEnd - filterStart} ms")
    val filterMaxDegree = filterNeighbors.map(_._2.length).max()
    println(s"max degree of filtered neighbors = $filterMaxDegree")

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

    println(">>> checkpoint filtered neighbor tables and edges")
    val cpStart = System.currentTimeMillis()
    filterNeighbors.foreachPartition(_ => Unit)
    filterNeighbors.checkpoint()
    edge.foreachPartition(_ => Unit)
    edge.checkpoint()
    val cpEnd = System.currentTimeMillis()
    println(s"checkpoint cost ${cpEnd - cpStart} ms")
    neighbors.unpersist()
    degrees.unpersist()

    println(">>> count triangles")
    val countStart = System.currentTimeMillis()
    val graph = Graph(filterNeighbors, edge)
    val totalClosedTriangle = EdgeIterPlusUndirected.computeNumOfClosedTriangle(graph, $(storageLevel))
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

object EdgeIterPlusUndirected {

  def filterNeighborTable(vid: VertexId,
                          nb: Array[VertexId],
                          degrees: Map[VertexId, Int]): Array[VertexId] = {
    val ret = new ArrayBuffer[VertexId]()
    val minDegree = degrees.getOrElse(vid, 0)
    nb.foreach { nid =>
      val nDegree = degrees.getOrElse(nid, 0)
      if (nDegree > minDegree) ret += nid
    }
    ret.toArray
  }

  def computeNumOfClosedTriangle(graphWithAdj: Graph[Array[VertexId], Byte],
                                 storageLevel: StorageLevel): Double = {
    graphWithAdj.triplets.mapPartitionsWithIndex { case (partId, iter) =>
      var count = 0
      var numTriplets = 0
      iter.foreach { edgeTriplet =>
        if (null != edgeTriplet.srcAttr && null != edgeTriplet.dstAttr
        && edgeTriplet.srcAttr.length >= 1 && edgeTriplet.dstAttr.length >= 1) {
          val minDegree = Math.min(edgeTriplet.srcAttr.length, edgeTriplet.dstAttr.length)
          val maxDegree = Math.max(edgeTriplet.srcAttr.length, edgeTriplet.dstAttr.length)
          if (minDegree >= 1 && maxDegree >= 2) {
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
        }
        numTriplets = numTriplets + 1
        if (numTriplets % 100000 == 0) println(s">>> $numTriplets triplets processed in partition $partId")
      }
      println(s">>> triangle counts in partition $partId = $count")
      Iterator.single(count)
    }.sum()
//      .flatMap { edgeTriplet =>
//      val srcNeighbors = edgeTriplet.srcAttr
//      val dstNeighbors = edgeTriplet.dstAttr
//      if (null == srcNeighbors || null == dstNeighbors
//        || srcNeighbors.length < 1 || dstNeighbors.length < 1) {
//        Iterator.empty
//      } else {
//        var i = 0
//        var j = 0
//        var count = 0L
//        while (i < srcNeighbors.length && j < dstNeighbors.length) {
//          if (srcNeighbors(i) == dstNeighbors(j)) {
//            count += 1
//            i += 1
//            j += 1
//          } else if (srcNeighbors(i) > dstNeighbors(j)) {
//            j += 1
//          } else {
//            i += 1
//          }
//        }
//        if (count > 0) Iterator.single(count) else Iterator.empty
//      }
//    }.sum()
  }

  def computeNumOfTriangleCandidates(degrees: RDD[Int]): Double = {
    degrees.map(d => d * (d - 1) / 2).sum()
  }
}
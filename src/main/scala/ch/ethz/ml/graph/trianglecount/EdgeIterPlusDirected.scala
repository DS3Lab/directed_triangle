package ch.ethz.ml.graph.trianglecount

import ch.ethz.ml.graph.data.{EdgeType, TriangleType, VertexId}
import ch.ethz.ml.graph.params._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.LoggerFactory
import org.apache.spark.storage.StorageLevel
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

class EdgeIterPlusDirected(override val uid: String) extends Transformer
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
      val src = row.getLong(0)
      val dst = row.getLong(1)
      Iterator((src, (dst, EdgeType.OUT)), (dst, (src, EdgeType.IN)))
    }.groupByKey(partNum).map { case (v, neighbors) =>
        (v, neighbors.toArray.distinct.filter(_._1 != v).groupBy(_._1).flatMap { case (b, flags) =>
          Iterator.single((b, EdgeType.fromFlag(flags.map(_._2)))) }.toArray.sortBy(_._1)
        )
      }
    val numVertex = neighbors.count()
    println(s"count of neighbor tables = $numVertex, number of partitions = ${neighbors.getNumPartitions}")
//    println(s"samples of neighbor RDD:")
//    neighbors.take(num = 2).foreach { case (src, neighbors) =>
//      println(s"src = $src, neighbors = ${neighbors.mkString(",")}") }
    val neighborEnd = System.currentTimeMillis()
    println(s"generate neighbors cost ${neighborEnd - neighborStart} ms")

    println(">>> generate vertex degrees")
    val degreeStart = System.currentTimeMillis()
    val degrees = neighbors.map { case (v, neighbors) =>
      (v, neighbors.length) }
//    println(s"samples of degrees:")
//    degrees.take(num = 2).foreach { case (src, degree) =>
//      println(s"src = $src, degree = $degree") }
    val maxDegree = degrees.map(_._2).max()
    println(s"max degree = $maxDegree")
    val bcDegrees = sc.broadcast(degrees.collect().toMap)
    val degreeEnd = System.currentTimeMillis()
    println(s"generate degrees cost ${degreeEnd - degreeStart} ms")

    println(">>> filter neighbors with degree")
    val filterStart = System.currentTimeMillis()
    val filterNeighbors = neighbors.map{ case (v, neighbors) =>
      (v, EdgeIterPlusDirected.filterNeighborTable(v, neighbors, bcDegrees.value))
    }.persist($(storageLevel))
    val numFilterNeighbors = filterNeighbors.count()
    println(s"count of filter neighbors = $numFilterNeighbors, number of partitions = ${filterNeighbors.getNumPartitions}")
    val filterNeighborsSize = filterNeighbors.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of filtered neighbor tables: max=${filterNeighborsSize.max}, min=${filterNeighborsSize.min}")
//    println(s"samples of filtered neighbor tables:")
//    filterNeighbors.take(num = 2).foreach { case (src, neighbors) =>
//      println(s"src = $src, neighbors = ${neighbors.mkString(",")}") }
    val filterEnd = System.currentTimeMillis()
    println(s"filter neighbors cost ${filterEnd - filterStart} ms")
    val filterMaxDegree = filterNeighbors.map(_._2.length).max()
    println(s"max degree of filtered neighbors = $filterMaxDegree")

    println(">>> load edges from the neighbors RDD")
    val edgeStart = System.currentTimeMillis()
    val edge = neighbors.flatMap{ case (src, neighbors) =>
        EdgeIterPlusDirected.getEdgesWithType2(src, neighbors).map { edge =>
          val pid = EdgePartition2D.getPartition(edge._1, edge._2, partNum)
          (pid, (edge._1, edge._2, edge._3))
        }
    }.partitionBy(new HashPartitioner(partNum))
      .map { case (_, (src, dst, eType)) =>
        Edge(src, dst, eType)
      }.persist($(storageLevel))
    val numEdge = edge.count()
    println(s"count of edge = $numEdge, number of partitions = ${edge.getNumPartitions}")
    val edgeSize = edge.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of edge: max=${edgeSize.max}, min=${edgeSize.min}")
//    println(s"samples of edge RDD:")
//    edge.take(num = 2).foreach { edge =>
//      println(s"src = ${edge.srcId}, dst = ${edge.dstId}, edge type = ${edge.attr}") }
    val edgeEnd = System.currentTimeMillis()
    println(s"generate edges cost ${edgeEnd - edgeStart} ms")

    println(">>> count edge tags")
    val countEdgeTagStart = System.currentTimeMillis()
    val outEdge = edge.filter(_.attr == 0x00)
    println(s"number of out edge = ${outEdge.count()}")
    val inEdge = edge.filter(_.attr == 0x01)
    println(s"number of in edge = ${inEdge.count()}")
    val biEdge = edge.filter(_.attr == 0x02)
    println(s"number of bi edge = ${biEdge.count()}")
    val countEdgeTagEnd = System.currentTimeMillis()
    println(s"count edge tags cost ${countEdgeTagEnd - countEdgeTagStart} ms")

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
    val triangleCounts = EdgeIterPlusDirected.computeNumOfClosedTriangle(graph, $(storageLevel))
    println(s"numTriangle=${triangleCounts.mkString(",")}")
    val countEnd = System.currentTimeMillis()
    println(s"count triangles cost ${countEnd - countStart} ms")

    val output = dataset
      .sparkSession
      .sparkContext
      .parallelize(Seq(
      Row(triangleCounts(0), triangleCounts(1), triangleCounts(2), triangleCounts(3),
        triangleCounts(4), triangleCounts(5), triangleCounts(6))
      ))

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(output, outputSchema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Array(
      StructField("Trans", LongType, nullable = false),
      StructField("OutRecip", LongType, nullable = false),
      StructField("InRecip", LongType, nullable = false),
      StructField("Cycle", LongType, nullable = false),
      StructField("OneRecip", LongType, nullable = false),
      StructField("TwoRecip", LongType, nullable = false),
      StructField("ThreeRecip", LongType, nullable = false)
    ))
  }
}

object EdgeIterPlusDirected {

  def filterNeighborTable(vid: VertexId,
                          neighbors: Array[(VertexId, Byte)],
                          degrees: Map[VertexId, Int]): Array[(VertexId, Byte)] = {
    val ret = new ArrayBuffer[(VertexId, Byte)]()
    val minDegree = degrees.getOrElse(vid, 0)
    neighbors.foreach { case (nid, tag) =>
      val nDegree = degrees.getOrElse(nid, 0)
      if (nDegree > minDegree) ret += ((nid, tag))
    }
    ret.toArray
  }

  // output Edge[], require OUT and BI (src<dst)
  def getEdgesWithType(src: VertexId, neighbors: Array[(VertexId, Byte)]): Iterator[Edge[Byte]] = {
    neighbors.flatMap { case (dst, eType) =>
      if (eType == EdgeType.IN)
        Iterator.empty
      else if (eType == EdgeType.BI && src > dst)
        Iterator.empty
      else
        Iterator.single(Edge(src, dst, eType))
    }.toIterator
  }

  // output triple (src,dst,tag), require OUT and BI (src<dst)
  def getEdgesWithType2(src: VertexId, neighbors: Array[(VertexId, Byte)]): Iterator[(VertexId, VertexId, Byte)] = {
    neighbors.flatMap { case (dst, eType) =>
      if (eType == EdgeType.IN)
        Iterator.empty
      else if (eType == EdgeType.BI && src > dst)
        Iterator.empty
      else
        Iterator.single((src, dst, eType))
    }.toIterator
  }

  def computeNumOfClosedTriangle(graphWithAdj: Graph[Array[(VertexId, Byte)], Byte],
                                 storageLevel: StorageLevel): Array[VertexId] = {
    graphWithAdj.triplets.mapPartitionsWithIndex { case (partId, iter) =>
      val count = new Array[Long](7)
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
              if (edgeTriplet.srcAttr(i)._1 == edgeTriplet.dstAttr(j)._1) {
                val triType = TriangleType.toTypeEdgeIter(edgeTriplet.attr, edgeTriplet.srcAttr(i)._2, edgeTriplet.dstAttr(j)._2)
                count(triType.toInt) += 1
                i += 1
                j += 1
              } else if (edgeTriplet.srcAttr(i)._1 > edgeTriplet.dstAttr(j)._1) {
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
      println(s">>> triangle counts in partition $partId = ${count.mkString(",")}")
      Iterator.single(count)
    }.reduce((c1, c2) => c1.zip(c2).map{ case (a, b) => a+b })
//      .flatMap { edgeTriplet =>
//      val srcNeighbors = edgeTriplet.srcAttr
//      val dstNeighbors = edgeTriplet.dstAttr
//      val edgeType = edgeTriplet.attr
//      val count = new Array[Long](7)
//      if (null == srcNeighbors || null == dstNeighbors
//        || srcNeighbors.length < 1 || dstNeighbors.length < 1) {
//        Iterator.single(count)
//      } else {
//        var i = 0
//        var j = 0
//        val count = new Array[Long](7)
//        while (i < srcNeighbors.length && j < dstNeighbors.length) {
//          if (srcNeighbors(i)._1 == dstNeighbors(j)._1) {
//            val triType = TriangleType.toTypeEdgeIter(edgeType, srcNeighbors(i)._2, dstNeighbors(j)._2)
//            count(triType.toInt) += 1
//            i += 1
//            j += 1
//          } else if (srcNeighbors(i)._1 > dstNeighbors(j)._1) {
//            j += 1
//          } else {
//            i += 1
//          }
//        }
//        Iterator.single(count)
//      }
//    }.reduce((c1, c2) => c1.zip(c2).map{ case (a, b) => a+b })
  }

  def computeNumOfTriangleCandidates(degrees: RDD[Int]): Double = {
    degrees.map(d => d * (d - 1) / 2).sum()
  }
}

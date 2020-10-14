package ch.ethz.ml.graph.trianglecount

import ch.ethz.ml.graph.data.VertexId
import ch.ethz.ml.graph.params._
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map

class NodeIterUndirected(override val uid: String) extends Transformer
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
    val neighborsSize = neighbors.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of neighbor tables: max=${neighborsSize.max}, min=${neighborsSize.min}")
    val neighborEnd = System.currentTimeMillis()
    println(s"generate neighbors cost ${neighborEnd - neighborStart} ms")

    println(">>> generate vertex degrees")
    val degreeStart = System.currentTimeMillis()
    val degrees = neighbors.map { case (v, neighbors) =>
      (v, neighbors.length) }
    val maxDegree = degrees.map(_._2).max()
    println(s"max degree = $maxDegree")
    val degreeEnd = System.currentTimeMillis()
    println(s"generate degrees cost ${degreeEnd - degreeStart} ms")
    val numTotalCandidates = NodeIterDirected.computeNumOfTriangleCandidates(degrees.map(_._2))
    println(s"total number of candidate edges = $numTotalCandidates")

    println(">>> load edges from neighbor tables")
    val edgeStart = System.currentTimeMillis()
    val edge = neighbors.flatMap { case (v, nb) =>
        nb.flatMap { nid =>
          if (nid > v) {
            val pid = EdgePartition2D.getPartition(v, nid, partNum)
            Iterator.single((pid, (v, nid, true)))
          } else
            Iterator.empty
        }
    }.partitionBy(new HashPartitioner(partNum))
      .map { case (_, (src, dst, flag)) =>
        ((src, dst), flag)
      }.persist($(storageLevel))
//    val edge = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.map { row =>
//      val src = math.min(row.getLong(0), row.getLong(1))
//      val dst = math.max(row.getLong(0), row.getLong(1))
//      val pid = EdgePartition2D.getPartition(src, dst, partNum)
//      (pid, (src, dst, true))
//    }.partitionBy(new HashPartitioner(partNum))
//      .map { case (_, (src, dst, eType)) =>
//        ((src, dst), eType)
//      }.persist($(storageLevel))
    println(s"count of edge = ${edge.count()}, number of partitions = ${edge.getNumPartitions}")
    val edgeSize = edge.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of edge: max=${edgeSize.max}, min=${edgeSize.min}")
    val edgeEnd = System.currentTimeMillis()
    println(s"generate edges cost ${edgeEnd - edgeStart} ms")

    println(">>> load candidate edges from the dataset")
    val candEdgeStart = System.currentTimeMillis()
    val edgeCandidates = neighbors.flatMap { case (v, nb) =>
      val candidates = NodeIterUndirected.candidateEdges(nb)
      if (candidates.isEmpty)
        Iterator.empty
      else
        candidates.map( a => ((a._1, a._2), 1)).toIterator
    }
//      .map { edge =>
//      val pid = EdgePartition2D.getPartition(edge._1._1, edge._1._2, partNum)
//      (pid, edge)
//    }.partitionBy(new HashPartitioner(partNum)).map(_._2)
    println(s"count of candidate edges = ${edgeCandidates.count()}, number of partitions = ${neighbors.getNumPartitions}")
    val edgeCandSize = edgeCandidates.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of candidate edge: max=${edgeCandSize.max}, min=${edgeCandSize.min}")
    val candEdgeEnd = System.currentTimeMillis()
    println(s"generate candidate edges cost ${candEdgeEnd - candEdgeStart} ms")

    println(">>> checkpoint edges")
    val cpStart = System.currentTimeMillis()
    edge.foreachPartition(_ => Unit)
    edge.checkpoint()
    val cpEnd = System.currentTimeMillis()
    println(s"checkpoint cost ${cpEnd - cpStart} ms")
    neighbors.unpersist()

    println(">>> count triangles")
    val countStart = System.currentTimeMillis()
    val totalClosedTriangle = edgeCandidates.leftOuterJoin(edge)
      .mapPartitionsWithIndex { (partId, iter) =>
        var partCount = 0
        var numCand = 0
        iter.foreach { case ((src, dst), (count, flag)) =>
          flag match {
            case Some(b) => partCount = partCount + count
            case None => Unit
          }
          numCand += 1
          if (numCand % 100000 == 0) println(s">>> $numCand candidate edges processed in partition $partId")
        }
      Iterator.single(partCount)
    }.sum()
//      .flatMap { case ((src, dst), (count, flag)) =>
//        flag match {
//          case Some(b) => Iterator.single(count)
//          case None => Iterator.empty
//        }
//    }.sum()
    println(s"numTriangle=${totalClosedTriangle / 3.0}")
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

object NodeIterUndirected {

  def candidateEdges(neighbors: Array[VertexId]): Array[(VertexId, VertexId)] = {
    val ret = new ArrayBuffer[(VertexId, VertexId)]()
    if (neighbors.length < 2)
      ret.toArray
    for (i <- 1 until neighbors.length) {
      for (j <- 0 until i) {
        ret += ((neighbors(j), neighbors(i)))
      }
    }
    ret.toArray
  }

  def computeNumOfClosedTriangle(graphWithAdj: Graph[Array[Long], Byte],
                                 bcDegrees: Map[VertexId, Int]): Double = {
    graphWithAdj.triplets.flatMap { edgeTriplet =>
      val srcId = edgeTriplet.srcId
      val dstId = edgeTriplet.dstId
      val srcDegree = bcDegrees.getOrElse(srcId, 0)
      val dstDegree = bcDegrees.getOrElse(dstId, 0)
      assert(srcDegree <= dstDegree)
      val srcNeighbors = edgeTriplet.srcAttr
      val dstNeighbors = edgeTriplet.dstAttr
      if (null == srcNeighbors) {
        Iterator.empty
      } else {
        var count = 0L
        if (srcNeighbors.contains(dstId))
          count += 1
        if (count > 0) Iterator.single(count) else Iterator.empty
      }
    }.sum()
  }

  def computeNumOfTriangleCandidates(degrees: RDD[Int]): Double = {
    degrees.map(d => d * (d - 1) / 2).sum()
  }
}



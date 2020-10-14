package ch.ethz.ml.graph.trianglecount

import ch.ethz.ml.graph.data.{EdgeType, TriangleType, VertexId}
import ch.ethz.ml.graph.params._
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

class NodeIterPlusDirected(override val uid: String) extends Transformer
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
      Iterator((row.getLong(0), (row.getLong(1), EdgeType.OUT)), (row.getLong(1), (row.getLong(0), EdgeType.IN)))
    }.groupByKey(partNum)
      .map { case (v, neighbors) =>
        (v, neighbors.toArray.distinct.filter(_._1 != v).groupBy(_._1).flatMap { case (b, flags) =>
          Iterator.single((b, EdgeType.fromFlag(flags.map(_._2)))) }.toArray.sortBy(_._1)
        )
      }
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
    val bcDegrees = sc.broadcast(degrees.collectAsMap())
    val degreeEnd = System.currentTimeMillis()
    println(s"generate degrees cost ${degreeEnd - degreeStart} ms")

    println(">>> load edges from the neighbors RDD")
    val edgeStart = System.currentTimeMillis()
    val edge = neighbors.flatMap{ case (src, neighbors) =>
      NodeIterPlusDirected.getEdgesWithType(src, neighbors)
        .map { edge =>
          val pid = EdgePartition2D.getPartition(edge._1, edge._2, partNum)
          (pid, (edge._1, edge._2, edge._3))
        }
    }.partitionBy(new HashPartitioner(partNum))
      .map { case (_, (src, dst, eType)) => ((src, dst), eType) }
      .persist($(storageLevel))
    println(s"count of edge = ${edge.count()}, number of partitions = ${edge.getNumPartitions}")
    val edgeSize = edge.mapPartitions(iter => Iterator(iter.size), preservesPartitioning = true).collect()
    println(s"partition size of edge: max=${edgeSize.max}, min=${edgeSize.min}")
    val edgeEnd = System.currentTimeMillis()
    println(s"generate edges cost ${edgeEnd - edgeStart} ms")

    println(">>> load candidate edges from neighbor tables")
    val candEdgeStart = System.currentTimeMillis()
    val edgeCandidates = neighbors.flatMap { case (v, nb) =>
      val candidates = NodeIterPlusDirected.candidateEdges(v, nb, bcDegrees.value)
      if (candidates.isEmpty)
        Iterator.empty
      else
        candidates.map( a => ((a._1, a._2), (a._3, a._4)))
    }
//      .map { edge =>
//      val pid = EdgePartition2D.getPartition(edge._1._1, edge._1._2, partNum)
//      (pid, edge)
//    }.partitionBy(new HashPartitioner(partNum)).map(_._2)
    println(s"count of candidate edges = ${edgeCandidates.count()}, number of partitions = ${edgeCandidates.getNumPartitions}")
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
    val triangleCounts = edgeCandidates.leftOuterJoin(edge)
      .mapPartitionsWithIndex { (partId, iter) =>
        val count = new Array[Long](7)
        var numCand = 0
        iter.foreach { case ((src, dst), ((srcType, dstType), ownType)) =>
          ownType match {
            case Some(v) => {
              val triType = TriangleType.toTypeNodeIter(v, srcType, dstType)
              count(triType.toInt) += 1
              Iterator.single(count)
            }
            case None => Unit
          }
          numCand += 1
          if (numCand % 100000 == 0) println(s">>> $numCand candidate edges processed in partition $partId")
        }
        Iterator.single(count)
      }.reduce((c1, c2) => c1.zip(c2).map{ case (a, b) => a+b })

    println(s"numTriangle=${triangleCounts.mkString(",")}")
    val countEnd = System.currentTimeMillis()
    println(s"count triangles cost ${countEnd - countStart} ms")

    val output = dataset.sparkSession.sparkContext
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

object NodeIterPlusDirected {

  // require src < dst
  def getEdgesWithType(src: VertexId, neighbors: Array[(VertexId, Byte)]): Iterator[(VertexId, VertexId, Byte)] = {
    neighbors.flatMap { case (dst, eType) =>
      if (src < dst) {
        Iterator.single((src, dst, eType))
      } else {
          Iterator.empty
      }
    }.toIterator
  }

  def candidateEdges(vid: VertexId,
                     neighbors: Array[(VertexId, Byte)],
                     degrees: Map[VertexId, Int]): Array[(VertexId, VertexId, Byte, Byte)] = {
    val ret = new ArrayBuffer[(VertexId, VertexId, Byte, Byte)]()
    val minDegree = degrees.getOrElse(vid, 0)
    if (minDegree < 2) {
      ret.toArray
    }
    for (i <- 1 until neighbors.length) {
      for (j <- 0 until i) {
        val srcVertex = neighbors(j)._1
        val dstVertex = neighbors(i)._1
        val srcDegree = degrees.getOrElse(srcVertex, 0)
        val dstDegree = degrees.getOrElse(dstVertex, 0)
        val srcEdgeType = neighbors(j)._2  // vid -> src
        val dstEdgeType = neighbors(i)._2  // vid -> dst
        if (srcDegree > minDegree && dstDegree > minDegree)
          ret.append((srcVertex, dstVertex, srcEdgeType, dstEdgeType))
      }
    }
    ret.toArray
  }

//  def candidateEdges(vid: VertexId,
//                     neighbors: Array[(VertexId, Byte)],
//                     degrees: Map[VertexId, Int]): Array[Edge[(Byte, Byte)]] = {
//    val ret = new ArrayBuffer[Edge[(Byte, Byte)]]()
//
//    val minDegree = degrees.getOrElse(vid, 0)
//    if (minDegree < 2) {
//      ret.toArray
//    }
//    for (srcVertex <- neighbors; dstVertex <- neighbors) {
//      if (srcVertex._1 < dstVertex._1) {
//        val srcDegree = degrees.getOrElse(srcVertex._1, 0)
//        val dstDegree = degrees.getOrElse(dstVertex._1, 0)
//        val srcEdgeType = srcVertex._2  // vid -> src
//        val dstEdgeType = dstVertex._2  // vid -> dst
//        if (srcDegree > minDegree && dstDegree > minDegree) {
//          //println(s"min degree = $minDegree, src degree = $srcDegree, dst degree=$dstDegree")
//          if (srcDegree <= dstDegree)
//            ret += Edge(srcVertex._1, dstVertex._1,
//              (srcEdgeType, dstEdgeType))
//          else
//            ret += Edge(dstVertex._1, srcVertex._1,
//              (dstEdgeType, srcEdgeType))
//        }
//        //        if (srcDegree == minDegree && dstDegree > minDegree
//        //          && srcVertex._1 < vid) {
//        //          ret += Edge(srcVertex._1, dstVertex._1,
//        //            (srcEdgeType, dstEdgeType))
//        //        } else if (dstDegree == minDegree && srcDegree > minDegree
//        //          && dstVertex._1 < vid) {
//        //          ret += Edge(dstVertex._1, srcVertex._1,
//        //            (dstEdgeType, srcEdgeType))
//        //        } else if (srcDegree == minDegree && dstDegree == minDegree
//        //          && dstVertex._1 < vid) {
//        //          ret += Edge(srcVertex._1, dstVertex._1,
//        //            (srcEdgeType, dstEdgeType))
//        //        } else if (srcDegree > minDegree && dstDegree > minDegree) {
//        //          if (srcDegree <= dstDegree) {
//        //            ret += Edge(srcVertex._1, dstVertex._1,
//        //              (srcEdgeType, dstEdgeType))
//        //          }
//        //          else {
//        //            ret += Edge(dstVertex._1, srcVertex._1,
//        //              (dstEdgeType, srcEdgeType))
//        //          }
//        //        }
//      }
//    }
//    ret.toArray
//  }

  def computeNumOfClosedTriangle(graphWithAdj: Graph[Array[(VertexId, Byte)], (Byte, Byte)],
                                 bcDegrees: Map[VertexId, Int]): Array[Long] = {
    graphWithAdj.triplets.flatMap { edgeTriplet =>
      val srcId = edgeTriplet.srcId
      val dstId = edgeTriplet.dstId
      val srcEdgeType = edgeTriplet.attr._1 // edge of src vertex (v -> src)
      val dstEdgeType = edgeTriplet.attr._2 // edge of dst vertex (v -> dst)
      val srcDegree = bcDegrees.getOrElse(srcId, 0)
      val dstDegree = bcDegrees.getOrElse(dstId, 0)
      assert(srcDegree <= dstDegree)
      val srcNeighbors = edgeTriplet.srcAttr.toMap
      val dstNeighbors = edgeTriplet.dstAttr
      if (null == srcNeighbors) {
        Iterator.empty
      } else {
        if (srcNeighbors.contains(dstId)) {
          val count = new Array[Long](7)
          val ownEdgeType = srcNeighbors(dstId) // src -> dst
          val triType = TriangleType.toTypeNodeIter(ownEdgeType, srcEdgeType, dstEdgeType)
          count(triType.toInt) += 1
          Iterator.single(count)
        } else {
          Iterator.empty
        }
      }
    }.reduce((c1, c2) => c1.zip(c2).map{ case (a, b) => a+b })
  }

  def computeNumOfTriangleCandidates(degrees: RDD[Int]): Double = {
    degrees.map(d => d * (d - 1) / 2).sum()
  }
}





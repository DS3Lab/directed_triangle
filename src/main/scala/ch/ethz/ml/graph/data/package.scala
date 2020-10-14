package ch.ethz.ml.graph

import scala.collection.mutable

package object data {

  type VertexId = Long

  type PartitionId = Int

  type VertexSet = mutable.HashSet[Long]

  type CounterTriangleDirected = Array[Int]
}

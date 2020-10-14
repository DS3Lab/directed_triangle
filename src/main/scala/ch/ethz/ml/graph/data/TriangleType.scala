package ch.ethz.ml.graph.data

object TriangleType {

  /**
   *        1
   *      /  \
   *    2 --- 3
   */
  val Trans: Byte = 0x00  // trans: 1->2, 3->2, 3->1
  val OutRecip: Byte = 0x01 // out-recip: 1->2, 2<->3, 1->3
  val InRecip: Byte = 0x02  // in-recip: 2->1, 2<->3, 3->1
  val Cycle: Byte = 0x03  // cycle: 1->2, 2->3, 3->1
  val OneRecip: Byte = 0x04  // 1-recip: 1->2, 2<->3, 3->1
  val TwoRecip: Byte = 0x05 // 2-recip: 1->2, 2<->3, 1<->3
  val ThreeRecip: Byte = 0x06 // 3-recip: 1<->2, 2<->3, 1<->3

  def numBiEdge(edge1Type: Byte, edge2Type: Byte, edge3Type: Byte): Int = {
    var num: Int = 0
    if (edge1Type == EdgeType.BI)
      num += 1
    if (edge2Type == EdgeType.BI)
      num += 1
    if (edge3Type == EdgeType.BI)
      num += 1
    num
  }

  def toTypeEdgeIter(edge1Type: Byte, edge2Type: Byte, edge3Type: Byte): Byte = {
    val biNum = numBiEdge(edge1Type, edge2Type, edge3Type)
    if (biNum == 0) {
      (edge1Type, edge2Type, edge3Type) match {
        case (0x00, 0x01, 0x00) => TriangleType.Cycle
        //case _ => TriangleType.Trans  // case (0, 1, 1) or (0, 0, 1) or (0, 0, 0)
        case (0x00, 0x01, 0x01) | (0x00, 0x00, 0x01) | (0x00, 0x00, 0x00) => TriangleType.Trans  // case (0, 1, 1) or (0, 0, 1) or (0, 0, 0)
      }
    } else if (biNum == 1) {
      (edge1Type, edge2Type, edge3Type) match {
        case (0x00, 0x00, 0x02) | (0x02, 0x01, 0x01) => TriangleType.OutRecip  // case (0,0,2) or (2,1,1)
        case (0x00, 0x02, 0x01) | (0x02, 0x00, 0x00) | (0x01, 0x02, 0x01) => TriangleType.InRecip // case (0,2,1) or (2,0,0) or (1,2,1)
        case (0x00, 0x01, 0x02) | (0x02, 0x01, 0x00) | (0x02, 0x00, 0x01) | (0x00, 0x02, 0x00) => TriangleType.OneRecip // case (0,1,2) or (2,1,0) or (2,0,1) or (0,2,0)
      }
    } else if (biNum == 2) {
      TriangleType.TwoRecip
    } else {
      TriangleType.ThreeRecip
    }
  }

  def toTypeNodeIter(ownType: Byte, srcEdgeType: Byte, dstEdgeType: Byte): Byte = {
    val biNum = numBiEdge(ownType, srcEdgeType, dstEdgeType)
    if (biNum == 0) {
      (ownType, srcEdgeType, dstEdgeType) match {
        case (0x00, 0x00, 0x00) => TriangleType.Trans
        case (0x00, 0x01, 0x00) => TriangleType.Trans
        case (0x00, 0x01, 0x01) => TriangleType.Trans
        case (0x00, 0x00, 0x01) => TriangleType.Cycle
        case (0x01, 0x00, 0x00) => TriangleType.Trans
        case (0x01, 0x00, 0x01) => TriangleType.Trans
        case (0x01, 0x01, 0x00) => TriangleType.Cycle
        case (0x01, 0x01, 0x01) => TriangleType.Trans
      }
    } else if (biNum == 1) {
      (ownType, srcEdgeType, dstEdgeType) match {
        case (0x00, 0x00, 0x02) => TriangleType.OneRecip
        case (0x00, 0x01, 0x02) => TriangleType.OutRecip
        case (0x00, 0x02, 0x00) => TriangleType.InRecip
        case (0x00, 0x02, 0x01) => TriangleType.OneRecip
        case (0x01, 0x00, 0x02) => TriangleType.InRecip
        case (0x01, 0x01, 0x02) => TriangleType.OneRecip
        case (0x01, 0x02, 0x00) => TriangleType.OneRecip
        case (0x01, 0x02, 0x01) => TriangleType.OutRecip
        case (0x02, 0x00, 0x00) => TriangleType.OutRecip
        case (0x02, 0x00, 0x01) => TriangleType.OneRecip
        case (0x02, 0x01, 0x00) => TriangleType.OneRecip
        case (0x02, 0x01, 0x01) => TriangleType.InRecip
      }
    } else if (biNum == 2) {
      TriangleType.TwoRecip
    } else {
      TriangleType.ThreeRecip
    }
  }

  def main(args: Array[String]): Unit = {
    TriangleType.toTypeEdgeIter(0x00, 0x00, 0x02)
  }
}

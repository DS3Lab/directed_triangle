package ch.ethz.ml.graph.data

object EdgeType {

  val OUT: Byte = 0x00
  val IN: Byte = 0x01
  val BI: Byte = 0x02

  def fromFlag(flags: Array[Byte]): Byte = {
    if (flags.length == 1) {
      if (flags(0) == 0x00)
        OUT
      else IN
    } else {
      BI
    }
  }

  def main(args: Array[String]): Unit = {
    val flag = 0x10
    println(flag)
  }
}

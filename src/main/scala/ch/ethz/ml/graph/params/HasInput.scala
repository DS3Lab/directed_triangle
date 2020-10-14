package ch.ethz.ml.graph.params

import org.apache.spark.ml.param.{Param, Params}

trait HasInput extends Params {

  final val input = new Param[String](this, "input", "input")

  final def getInput: String = $(input)

  setDefault(input, null)

  final def setInput(in: String): this.type = set(input, in)

}

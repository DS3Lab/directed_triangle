package ch.ethz.ml.graph.data

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

object GraphIO {

  private val DELIMITER = "delimiter"
  private val HEADER = "header"


  def load(input: String, isWeighted: Boolean, isHeader: Boolean = false,
           srcIndex: Int = 0, dstIndex: Int = 1, weightIndex: Int = 2,
           sep: String = " "): DataFrame = {
    val ss = SparkSession.builder().getOrCreate()
    val schema = if (isWeighted) {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("weight", FloatType, nullable = false)
      ))
    } else {
      StructType(Seq(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false)
      ))
    }
    ss.read
      .option("sep", sep)
      .option("header", isHeader)
      .schema(schema)
      .csv(input)
  }

  def save(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    df.write
      .mode(SaveMode.Overwrite)
      .option(HEADER, "false")
      .option(DELIMITER, seq)
      .csv(output)
  }

  def defaultCheckpointDir: Option[String] = {
    val sparkContext = SparkContext.getOrCreate()
    sparkContext.getConf.getOption("spark.yarn.stagingDir")
      .map { base =>
        new Path(base, s".sparkStaging/${sparkContext.getConf.getAppId}").toString
      }
  }
}
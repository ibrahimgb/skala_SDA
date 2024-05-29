package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

case class JsonReader(path: String)
  extends Reader {
  val format = "json"

  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read.format(format)
      .load(path)
  }
}

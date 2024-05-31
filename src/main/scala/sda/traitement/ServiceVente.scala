package sda.traitement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter(): DataFrame = {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }
    def calculTTC(): DataFrame = {
      // Define a UDF to calculate TTC
      val calculateTTC = udf((htt: String, tva: String) => {
        val httValue = htt.replace(',', '.').toFloat
        val tvaValue = tva.replace(',', '.').toFloat
        BigDecimal(httValue + (tvaValue * httValue)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
      })

      // Apply the UDF to calculate TTC and add it as a new column
      dataFrame
        .formatter()
        .withColumn("TTC", calculateTTC(col("HTT"), col("TVA")))
        .drop("HTT_TVA")
    }

    def extractDateEndContratVille(): DataFrame = {
      // Define the schema for MetaTransaction
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, true)
        .add("Date_End_contrat", StringType, true)

      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

      // Extract Date_End_contrat and Ville from MetaData using from_json function
      val dfWithMeta = dataFrame.withColumn("MetaTransaction", from_json(col("MetaData"), schema))

      // Select Date_End_contrat and Ville from MetaTransaction and format the date
      val dfWithDateVille = dfWithMeta.withColumn("Date_End_contrat", date_format(to_date(expr("MetaTransaction[0].Date_End_contrat"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"))
        .withColumn("Ville", expr("MetaTransaction[0].Ville"))

      dfWithDateVille
    }

    def contratStatus(): DataFrame = {
      // Add Contrat_Status column based on Date_End_contrat comparison with the current date
      dataFrame.withColumn("Contrat_Status", when(col("Date_End_contrat") < current_date(), "Expired").otherwise("Actif"))
    }


  }

}
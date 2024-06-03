package sda.traitement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import scala.util.parsing.json._
import java.time.LocalDate
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


    def extractDateEndContratVille() = {
    // Define the schema for the nested MetaTransaction


    val schema_MetaTransaction = new StructType()
      .add("Ville", StringType, false)
      .add("Date_End_contrat", StringType, false)

    val schema = new StructType()
      .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

    // Parse the MetaData JSON column
    val parsedDF = dataFrame.withColumn("ParsedMetaData", from_json(col("MetaData"), schema))

    // Extract the Date_End_contrat field
    val extractedDF = parsedDF.withColumn("Date_End_contrat", expr("filter(ParsedMetaData.MetaTransaction, x -> x.Date_End_contrat IS NOT NULL)[0].Date_End_contrat")).drop("ParsedMetaData")

    // Drop the ParsedMetaData column if it's no longer needed
    // val finalDF = extractedDF.drop("ParsedMetaData")

      extractedDF
  }



    def contratStatus(): DataFrame = {
      // Définir le schéma pour le JSON imbriqué MetaTransaction
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, false)
        .add("Date_End_contrat", StringType, false)

      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

      val parsedDF = dataFrame.withColumn("ParsedMetaData", from_json(col("MetaData"), schema))


      val extractedDF = parsedDF.withColumn("Date_End_contrat", expr("filter(ParsedMetaData.MetaTransaction, x -> x.Date_End_contrat IS NOT NULL)[0].Date_End_contrat"))


      val currentDate = LocalDate.now().toString

      val finalDF = extractedDF.withColumn("Contrat_Status",
        when(to_date(col("Date_End_contrat")).lt(lit(currentDate)), "Expired")
          .otherwise("Actif"))

     finalDF.drop("ParsedMetaData")
      println("is showing?")
finalDF.show()
      finalDF
    }

  }

}
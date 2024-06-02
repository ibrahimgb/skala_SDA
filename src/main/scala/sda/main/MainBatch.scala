package sda.main

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import sda.args._
import sda.reader.{CsvReader, Reader}
import sda.parser.{ConfigurationParser, FileReaderUsingIOSource}
import sda.traitement.ServiceVente._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import sda.args._
import sda.reader.CsvReader
import sda.parser.FileReaderUsingIOSource
import sda.traitement.ServiceVente._


object MainBatch {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("SDA")
      .config("spark.master", "local")
      .getOrCreate()
    Args.parseArguments(args)

    Args.parseArguments(args)
    val  df: DataFrame= Args.readertype match {
      case "csv" => {
        val reader = ConfigurationParser.getCsvReaderConfigurationFromJson(Args.readerConfigurationFile)

        println("csv reader is:")
        println(reader)
        // Extract CSV reading configurations from CsvReaderConfig object
        val csvFilePath = reader.path
        val delimiter = reader.delimiter.getOrElse(",")
        val header = reader.header.getOrElse(false)
        // Read the CSV file into a DataFrame
        val df: DataFrame = spark.read
          .option("delimiter", delimiter)
          .option("header", header.toString)
          .csv(csvFilePath)
        df
      }
      case "json" => {
        val reader = ConfigurationParser.getJsonReaderConfigurationFromJson(Args.readerConfigurationFile)

        // Extract JSON reading configurations from JsonReaderConfig object
        // Parse JSON string to JsonReader object
        // Extract JSON reading configurations from JsonReader object

        val jsonFilePath = reader.path
        //val multiline = reader.multiline

        // Read the JSON file into a DataFrame
        val df: DataFrame = spark.read
          .option("multiline", true)
          .json(jsonFilePath)
        df
      }
      case _ => throw new Exception("Invalid reader type. Supported reader format : csv, json and xml in feature")
    }


    // Show the DataFrame content
    df.show(20)

        println("***********************Resultat Question1*****************************")
        df.show(20)
        println("***********************Resultat Question2*****************************")
        df.calculTTC().show(20)
        println("***********************Resultat Question3*****************************")
        df.extractDateEndContratVille().show(20)
        println("***********************Resultat Question4*****************************")
        df.contratStatus()

  }
}
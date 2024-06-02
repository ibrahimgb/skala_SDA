package sda.parser
import scala.io.Source
import upickle.default._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import sda.reader._
import org.json.JSONObject

object ConfigurationParser {

  // Implicit formats for JSON serialization/deserialization
  implicit val formats = DefaultFormats

  def getCsvReaderConfigurationFromJson(jsonUrl: String): CsvReader = {
    val jsonString = FileReaderUsingIOSource.getContent(jsonUrl)
    JsonMethods.parse(jsonString).extract[CsvReader]
  }
  /* Complétez cette fonction. Elle prend un String en argument et renvoie un objet JsonReader.
     Elle doit être codée de la même manière que la fonction getCsvReaderConfigurationFromJson // Done
  */

  def getJsonReaderConfigurationFromJson(jsonUrl: String) = {
    println("reading json data")
    val config = FileReaderUsingIOSource.getContent(jsonUrl)
    println(config)
    val json: ujson.Value  = ujson.read(config)
    println()

    val data = FileReaderUsingIOSource.getContent(json("path").str)

    data
    //println(jsonString)
   // val parsed =  JsonMethods.parse(jsonString).extract[JsonReader]
   // println(parsed)
  }
}

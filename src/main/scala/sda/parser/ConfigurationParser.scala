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

    // Parse JSON string
    val json = new JSONObject(config)

    // Access the content of "path" key
    val path = json.getString("path")

    // Print the content of "path"
    println(path)
    val data = FileReaderUsingIOSource.getContent("src/main/resources/DataForTest/data.json")
    println(data)

    data
   // println(jsonString)
   // val parsed =  JsonMethods.parse(jsonString).extract[JsonReader]
   // println(parsed)
  }
}

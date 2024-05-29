package sda.parser

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import sda.reader._

object ConfigurationParser {

  // Implicit formats for JSON serialization/deserialization
  implicit val formats = DefaultFormats

  def getCsvReaderConfigurationFromJson(jsonString: String): CsvReader = {
    JsonMethods.parse(jsonString).extract[CsvReader]
  }

  /* Complétez cette fonction. Elle prend un String en argument et renvoie un objet JsonReader.
     Elle doit être codée de la même manière que la fonction getCsvReaderConfigurationFromJson // Done
  */
  def getJsonReaderConfigurationFromJson(jsonString: String): JsonReader = {
    JsonMethods.parse(jsonString).extract[JsonReader]
  }
}

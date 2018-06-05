package github

import java.io.File
import java.net.URL
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, YearMonth}

import org.apache.spark.sql.{DataFrame, SaveMode}
import scala.sys.process._

class HelperFunctions {

  def saveData(dataFrame: List[DataFrame], fileFormat: String, catalogName: List[String]): Unit = {
    for (data <- dataFrame.zip(catalogName)) {
      val savePath = "output-data/" + data._2
      data._1.coalesce(1)
        .write
        .format(fileFormat)
        .mode(SaveMode.Append)
        .save(savePath)
    }
  }

  /**
    * Creates a {@code URL} object from the {@code String}
    * representation and saves it to a File
    *
    * @param      url      the { @code String} to parse as a URL.
    * @param      filename the { @code String} to save file with given name
    */
  def downloadSingleFile(url: String, filename: String): String = {
    new URL(url) #> new File(s"$filename") !!;
    filename
  }

  def downloadMonthlyData(year: Int, month: Int): String = {
    var fileName: String = null
    var data: LocalDateTime = LocalDateTime.of(year, month, 1, 0, 0)
    var url: String = null
    val path: String = "input-data/" + data.format(DateTimeFormatter.ofPattern("yyyy-MM")) + "/"
    new File(path).mkdirs()

    while (data.getDayOfMonth.compareTo(YearMonth.of(year, month).lengthOfMonth()) <= -1) {
      fileName = data.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-H")) + ".json.gz"
      url = "http://data.gharchive.org/" + fileName
      data = data.plusHours(1)
      if (!new File(path + fileName).exists){
        downloadSingleFile(url, path + fileName)
      }
    }

    path + "*.json.gz"
  }
}

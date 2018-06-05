import java.nio.file.{Files, Paths}

import core.AnalyzeGithubData
import github.HelperFunctions
import org.scalatest.FunSuite

import scala.reflect.io.File

class AnalyzeGithubDataTests extends FunSuite {

 test("Download Data Test"){
   val fileName = new HelperFunctions().downloadSingleFile("http://data.gharchive.org/2015-01-01-15.json.gz","2015-01-01-15.json.gz")
   assert(fileName == "2015-01-01-15.json.gz" && File("2015-01-01-15.json.gz").exists)
 }

 test("Save Data Test"){
    import core.AnalyzeGithubData.spark.implicits._
     val testDF = Seq(
      (1, "Mariusz"),
      (2, "Arek"),
      (3, "Bartek")
    ).toDF("id", "imie")

    new HelperFunctions().saveData(List(testDF),"parquet",List("TestData"))
    assert(AnalyzeGithubData.spark.read.parquet("output-data/TestData/*.parquet").count() > 0)
  }
}

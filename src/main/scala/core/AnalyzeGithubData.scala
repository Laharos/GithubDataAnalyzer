package core

import github.{HelperFunctions, Repositories, Users}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AnalyzeGithubData extends HelperFunctions {

  /**
    * To deploy application you have to have installed build tool called SBT
    * To build application into singe FAT .JAR file write "sbt assembly"
    * If you have spark libs already installed add adnotation "provided" in build.sbt
    * libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"
    *
    * To run application use spark-submit jarFileName, remember to switch to catalog where
    * your .jar file exist if you want have right path to save files
    *
    * Warning ! Before you start application remember to have enough space on disk
    * cause avg weight of single .json file for 2018 is 100MB+ per file which gives you
    * 24 * ~~100 * 30 = 72 000 MB
    * If you dont want to download whole month you can use github.HelperFunctions#downloadSingleFile(java.lang.String, java.lang.String)} function
    */

  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("http.agent", "Chrome")

  val spark: SparkSession = SparkSession.builder()
    .appName("allegro")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit ={
  /**
    * If you want do fast testing you can use first line to download single file.
    * If not use second line with Monthly Data but remember if may take long time due to required files and thier size
    */
  val catalogNames: List[String] = List[String]("repository", "users")
  //val df: DataFrame = spark.read.json(downloadSingleFile("http://data.gharchive.org/2018-03-01-15.json.gz", "2018-03-01-15.json.gz")).cache()
  val df: DataFrame = spark.read.json(downloadMonthlyData(2018, 3)).cache()
    val dfQueue: List[DataFrame] = List[DataFrame](
      new Repositories(df).generateRepositoryDataset,
      new Users(df).generateUserDataset
    )

    saveData(dfQueue, "parquet", catalogNames)

    spark.stop()
  }
}

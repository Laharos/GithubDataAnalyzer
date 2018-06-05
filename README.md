Allegro recruitment task

Author: Bartosz Czosnyk

Requirements: 
- Java 8 SDK
- Apache Spark 2.3
- SBT

Application is ready to deploy

2 ways:

Load application to IDE (intellij idea eg => Import project => select sbt file), application should download required dependencies

OR

To deploy application you have to have installed build tool called SBT
To build application into single FAT .JAR file select catalog with source files and write "sbt assembly" in cmd.
If you have spark libs already installed add adnotation "provided" in build.sbt libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"


To run application use spark-submit jarFileName, remember to switch to catalog where
your .jar file exist if you want have right path to save files

Warning ! Before you start application remember to have enough space on disk
cause avg weight of single .json file for 2018 is 100MB+ per file which gives you
24 * ~~100 * 30 = 72 000 MB
If you dont want to download whole month you can use HelperFunctions#downloadSingleFile(java.lang.String, java.lang.String)} function

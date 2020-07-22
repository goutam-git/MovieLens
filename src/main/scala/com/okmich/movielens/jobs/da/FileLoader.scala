package com.okmich.movielens.jobs.da


import java.nio.charset.Charset

import com.okmich.movielens.model.conf.Environment
import com.okmich.movielens.model.conf.Environment.EnvironmentEnum
import com.okmich.movielens.model.da.{Movies, Ratings, Tags}
import com.okmich.movielens.utils.SparkUtils.{loadSpark}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object FileLoader {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val environment  : EnvironmentEnum = Environment.fromString(System.getProperty("ENVIRONMENT", Environment.LOCAL.name))
  val config :Config  = ConfigFactory.load("movie").getConfig(environment.name)
  val master :String = config.getString("spark.master")
  val warehouse :String  = config.getString("spark.warehouse")
  val source :String = config.getString("spark.source")
  val moviesFile :String = config.getString(s"spark.${source}.reference.movies")
  val tagsFile :String = config.getString(s"spark.${source}.reference.tags")
  val ratingsFile :String = config.getString(s"spark.${source}.reference.ratings")
  val schema = config.getString(s"spark.${source}.db.schema")
  val mov_tbl = config.getString(s"spark.${source}.db.da.mov_da_tbl")

  def apply(spark: SparkSession)  = new FileLoader(spark)


  def main(args: Array[String]): Unit = {
    val spark : SparkSession = loadSpark(environment, master, warehouse)
    Try (FileLoader(spark).loadFiles()) match  {
      case Success(value) => {
        logger.info("Process Completed Sucessfuly")
        spark.stop()
      }
      case Failure(exception) => {
        exception.printStackTrace();
        spark.stop()
      }
    }
  }

}

class FileLoader(spark: SparkSession) {

  import FileLoader._
  import spark.implicits._

  def loadFiles(): Unit = {
    val moviesDS : Dataset[Movies] = readCsv[Movies](moviesFile) // exclude header
    moviesDS.show()

    val tagsDS : Dataset[Tags] = readCsv[Tags](tagsFile)
    //tagsDS.show()

    val ratingsDS : Dataset[Ratings] = readCsv[Ratings](ratingsFile)
   // ratingsDS.show()


    // check if any user has given duplicate ratings
   /* val dupRatingDs = ratingsDS.groupBy(Movies.MOVIE_ID,Tags.USER_ID).count()
                         .orderBy(Tags.USER_ID,Movies.MOVIE_ID)

    val filterdup = dupRatingDs.filter(dupRatingDs("count") > 1)

    filterdup.show()*/
    //save
    logger.info("schema---->"+ schema)
    logger.info("table--->"+ mov_tbl)
    //SparkUtils.saveDSTable[Movies](moviesDS,schema,mov_tbl)


    // gist to include header and filter it
  /*  val movieDS_withHeader :Dataset[Movies] = readMovieCsv[Movies](moviesFile) // include header
    val movieDS_withHeader_first = movieDS_withHeader.first() // first row
    val movieDS_header_filter = movieDS_withHeader.filter( row => row != movieDS_withHeader_first) */// filter header

  }

  def readCsv[T] (path: String)(implicit  encoder: Encoder[T]): Dataset[T] = {
    spark
      .read
      .option("header", "true")
      .csv(path)
      .as[T]
  }

  def readMovieCsv[T] (path: String)(implicit  encoder: Encoder[T]): Dataset[T] = {
    spark
      .read
      .option("header", "false")
      .csv(path)
      .withColumnRenamed("_c0", "movieId")
      .withColumnRenamed("_c1", "title")
      .withColumnRenamed("_c2", "genres")
      .as[T]
  }

  //find duplicates
  // group by movie id agg count

}
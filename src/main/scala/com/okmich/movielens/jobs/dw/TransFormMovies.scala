package com.okmich.movielens.jobs.dw

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.okmich.movielens.utils.SparkUtils.loadSpark
import com.okmich.movielens.utils.DateUtils
import com.okmich.movielens.model.conf.Environment
import com.okmich.movielens.model.conf.Environment.EnvironmentEnum
import com.okmich.movielens.model.da.{Movies, Ratings}
import com.okmich.movielens.model.dw.MovieTypes
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions.to_timestamp
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

object TransFormMovies extends MovieLensConfig {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val moviesFile :String = config.getString(s"spark.${source}.reference.movies")
  val ratingsFile:String = config.getString(s"spark.${source}.reference.ratings")
  val schema = config.getString(s"spark.${source}.db.schema")
  val mov_tbl = config.getString(s"spark.${source}.db.da.mov_da_tbl")
  val ratings_tbl = config.getString(s"spark.${source}.db.da.ref_da_tbl")
  val mov_trnsfrm_tbl = config.getString(s"spark.${source}.db.dw.mov_dw_tbl")
  val fmt = DateTimeFormat forPattern "yyyy-mm-dd HH:mm:ss.SSS"


  def apply(spark:SparkSession) = new TransFormMovies(spark)

  def main(args: Array[String]): Unit = {
    val spark : SparkSession = loadSpark(environment, master, warehouse)
    Try (TransFormMovies(spark).workflow()) match  {
      case Success(value) => {
        logger.info("Process Completed Sucessfuly")
        spark.stop()
      }
      case Failure(exception) => {
        exception.printStackTrace()
        spark.stop()
      }
    }
  }
}
class TransFormMovies(spark: SparkSession) extends Serializable {

  import TransFormMovies._
  import spark.implicits._


  def workflow(): Unit = {
    logger.info("schema--->" + schema)
    logger.info("tbl--->" + mov_trnsfrm_tbl)
    val selectStr:String = s"select *"
    //val movieDS : Dataset[Movies] = executeQuery[Movies](spark.sqlContext,schema,mov_tbl,10,selectStr)  // cannot use hive locally
    //val ratingsDS: Dataset[Ratings] = executeQuery(spark.sqlContext,schema,ratings_tbl,10,selectStr)// hence getting from file
    val movieDS : Dataset[Movies] = readCsv[Movies](moviesFile)
    val ratingsDS : Dataset[Ratings] = readCsv[Ratings](ratingsFile)

    val transFormDS : Dataset[MovieTypes] = transformMovies(movieDS,ratingsDS)
    transFormDS.show
  }

  def transformMovies(movieDS: Dataset[Movies],
                      ratingsDS: Dataset[Ratings]) : Dataset[MovieTypes] = {

    val genreDF: DataFrame = addGenre(movieDS)
    val ratingYrDF: DataFrame = addYear(ratingsDS)
    val transFormDF : DataFrame = trnsfrmMovies(genreDF,ratingYrDF)
    transFormDF.as[MovieTypes]
  }

  def executeQuery[T](sqlContext: SQLContext,
                      schema: String,
                      tbl: String,
                      partition: Int,
                      selClause:String)
                     (implicit  encoder: Encoder[T]): Dataset[T] = {
    sqlContext.setConf("spark.sql.shuffle.partitions", partition.toString)
    val fromClause = s"from ${schema}.${tbl}"
    val movieDF: DataFrame = sqlContext.sql(selClause.concat(fromClause))
    val movieDS: Dataset[T] = movieDF.as[T]
    movieDS
  }


  def readCsv[T] (path: String)(implicit  encoder: Encoder[T]): Dataset[T] = {
    spark
      .read
      .option("header", "true")
      .csv(path)
      .as[T]
  }

  def addGenre(movies: Dataset[Movies]) : DataFrame = {

    val genreUDF : UserDefinedFunction = udf(isGenre(_:String,_:String))
    val genreDF : DataFrame = movies.withColumn("isAction",genreUDF(col("genres"),lit("Action")))
                                    .withColumn("isAdventure",genreUDF(col("genres"),lit("Adventure")))
                                    .withColumn("isAnimation",genreUDF(col("genres"),lit("Animation")))
                                    .withColumn("isChildren",genreUDF(col("genres"),lit("Children")))
                                    .withColumn("isComedy",genreUDF(col("genres"),lit("Comedy")))
                                    .withColumn("isCrime",genreUDF(col("genres"),lit("Crime")))
                                    .withColumn("isDocumentary",genreUDF(col("genres"),lit("Documentary")))
                                    .withColumn("isDrama",genreUDF(col("genres"),lit("Drama")))
                                    .withColumn("isFantasy",genreUDF(col("genres"),lit("Fantasy")))
                                    .withColumn("isFilmNoir",genreUDF(col("genres"),lit("FilmNoir")))
                                    .withColumn("isHorror",genreUDF(col("genres"),lit("Horror")))
                                    .withColumn("isIMAX",genreUDF(col("genres"),lit("IMAX")))
                                    .withColumn("isMusical",genreUDF(col("genres"),lit("Musical")))
                                    .withColumn("isMystery",genreUDF(col("genres"),lit("Mystery")))
                                    .withColumn("isRomance",genreUDF(col("genres"),lit("Romance")))
                                    .withColumn("isScifi",genreUDF(col("genres"),lit("Scifi")))
                                    .withColumn("isThriller",genreUDF(col("genres"),lit("Thriller")))
                                    .withColumn("isWar",genreUDF(col("genres"),lit("War")))
                                    .withColumn("isWestern",genreUDF(col("genres"),lit("Western")))
    genreDF

  }

  def addYear(ratings: Dataset[Ratings]) : DataFrame = {
    val yearUDF : UserDefinedFunction = udf(ratingYear(_:String))
    val ratingYrDF : DataFrame = ratings.withColumn("Year",yearUDF(col("timestamp")))
    ratingYrDF
  }

  def trnsfrmMovies(genreDF: DataFrame,ratingYrDF:DataFrame) : DataFrame ={
    val tansformDF: DataFrame = genreDF.join(ratingYrDF,genreDF("movieId") === ratingYrDF("movieId"),"inner")
      .drop(col("genres"))
      .drop(col("timestamp"))
      .drop(col("userId"))
      .drop(ratingYrDF("movieId"))
      .drop(col("rating"))
      .withColumnRenamed("movieId","id")
      .dropDuplicates()

    val orderDF =  tansformDF.select(col("id"),col("title"),col("year"),
                      col("isAction"),col("isAdventure"),col("isAnimation"),
                      col("isChildren"),col("isComedy"),col("isCrime") ,
                      col("isDocumentary"),col("isDrama"),col("isFantasy"),
                      col("isFantasy"),col("isFilmNoir"),col("isHorror"),
                      col("isIMAX"),col("isMusical"),col("isMystery"),
                      col("isRomance"),col("isScifi"),col("isThriller"),
                      col("isWar"),col("isWestern"))


    orderDF
  }

  def isGenre(genres: String, genre:String) : Boolean = {
    return genres.contains(genre)
  }

  def ratingYear(timeStampStr:String) : Int = {
    val ts = timeStampStr.contains(":") match {
      case true => new Timestamp((fmt  parseDateTime timeStampStr).getMillis).getTime
      case _ => timeStampStr.toLong
    }
    val year :Int  = DateUtils(ts,"year")
    year
  }

  def parseDate(input: String):String = try {
    (fmt parseDateTime input).getMillis.toString
  } catch {
    case e: IllegalArgumentException => input
  }


}

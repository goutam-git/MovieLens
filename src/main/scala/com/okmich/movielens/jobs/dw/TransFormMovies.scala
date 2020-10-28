package com.okmich.movielens.jobs.dw

import java.sql.Timestamp

import com.okmich.movielens.model.da.{Movies, Ratings, Tags}
import com.okmich.movielens.model.dw.MovieTypes
import com.okmich.movielens.utils.DateUtils
import com.okmich.movielens.utils.DateUtils._
import com.okmich.movielens.utils.SparkUtils.loadSpark
import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, regexp_extract, udf}
import org.joda.time.format.DateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

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
    Try (TransFormMovies(spark).workflow) match  {
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
  implicit val tagsEncoder: Encoder[Tags] = Encoders.product[Tags]

  def workflow(): Unit = {
    logger.info("schema--->" + schema)
    logger.info("tbl--->" + mov_trnsfrm_tbl)
    val selectStr:String = s"select *"
    //val movieDS : Dataset[Movies] = executeQuery[Movies](spark.sqlContext,schema,mov_tbl,10,selectStr)  // cannot use hive locally
    //val ratingsDS: Dataset[Ratings] = executeQuery(spark.sqlContext,schema,ratings_tbl,10,selectStr)// hence getting from file
    val movieDS : Dataset[Movies] = readCsv[Movies](moviesFile)

    val transFormDS : Dataset[MovieTypes] = transformMovies(movieDS)
    transFormDS.show
  }

  def transformMovies(movieDS: Dataset[Movies]) : Dataset[MovieTypes] = {

    val genreDF: DataFrame = addGenre(movieDS)
    val yearDF: DataFrame = addYear(genreDF)
    val transFormDF : DataFrame = trnsfrmMovies(yearDF)
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

  def addYear(genresDF: DataFrame) : DataFrame = {
    val genreYearDF : DataFrame = genresDF.withColumnRenamed("movieId","id")
      .withColumn("year", regexp_extract(col("title"), "\\((.*?)\\)", 1).cast("Integer"))
    genreYearDF
  }

  def trnsfrmMovies(genreYearDF:DataFrame) : DataFrame ={

    val orderDF =  genreYearDF.select(col("id"),col("title"),col("year"),
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

  def readCsv[T] (path: String)(implicit  encoder: Encoder[T]): Dataset[T] = {
    spark
      .read
      .option("header", "true")
      .csv(path)
      .as[T]
  }


}

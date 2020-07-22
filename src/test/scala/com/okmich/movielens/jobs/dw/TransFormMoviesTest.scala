package com.okmich.movielens.jobs.dw

import java.sql.Timestamp

import com.okmich.movielens.jobs.JobTest
import com.okmich.movielens.model.da.{Movies, Ratings}
import com.okmich.movielens.model.dw.MovieTypes
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}
import org.mockito.Mockito._
import org.mockito.Mockito


class TransFormMoviesTest  extends JobTest{
  val now: Long = System.currentTimeMillis()
  val nowDate: Int = 20200701
  val nowTimestamp: Timestamp = new Timestamp(now)
  implicit val movTypEncoder: Encoder[MovieTypes] = Encoders.product[MovieTypes]
  implicit val movEncoder: Encoder[Movies] = Encoders.product[Movies]
  implicit val ratingsEncoder: Encoder[Ratings] = Encoders.product[Ratings]

  "TransFormMovies" should " return  movie dataset by extracting the genres from Movies and Year from Ratings" in {

   val movieList: List[Movies] = List(
      Movies("1","Toy Story (1995)","Adventure|Animation|Children|Comedy|Fantasy"),
      Movies("2","Jumanji (1995)","Adventure|Children|Fantasy")
   )

   val ratingsList: List[Ratings]  = List(
     Ratings("1","1","3.5",nowTimestamp),
     Ratings("1","2","3.5",nowTimestamp)
   )

    val moviesDS : Dataset[Movies] = createDataSet[Movies](movieList)

    val ratingsDS: Dataset[Ratings] = createDataSet[Ratings](ratingsList)


    val output : List[MovieTypes] = List(
      MovieTypes("2", "Jumanji (1995)", 2020,false,true,false,true,false,false,false,false,true,false,false,false,false,false,false,false,false,false,false),
      MovieTypes("1", "Toy Story (1995)", 2020,false,true,true, true,true,false,false,false,true,false,false,false,false,false,false,false,false,false,false)
    )

    val movieDF : DataFrame = createDataFrame(movieList)
    val expectedOutput : Dataset[MovieTypes] = createDataSet[MovieTypes](output)

    System.err.println("Movies DataFrame" + printDataFrame(movieDF))
    System.err.println("Movies Dataset: " + printDataSet[Movies](moviesDS))
    System.err.println("Ratings Dataset: " + printDataSet[Ratings](ratingsDS))
    System.err.println("Expected Output: " + printDataSet[MovieTypes](expectedOutput))


    val job = new TransFormMovies(spark)
    val mockJob = mock(classOf[TransFormMovies],withSettings().spiedInstance(job).serializable
      .defaultAnswer(Mockito.CALLS_REAL_METHODS))
    doReturn(moviesDS).when(mockJob).readCsv[Movies]("testpath")
    doReturn(ratingsDS).when(mockJob).readCsv[Ratings]("testpath")

    val result :  Dataset[MovieTypes]  = mockJob.transformMovies(moviesDS,ratingsDS)

    System.err.println("result: " + printDataSet[MovieTypes](result))

    result.collect should contain theSameElementsAs expectedOutput.collect

    expectedOutput should beEqualToDataSet(result)

  }
  
}

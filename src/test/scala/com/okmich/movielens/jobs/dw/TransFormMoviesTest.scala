package com.okmich.movielens.jobs.dw


import java.time.Instant

import com.okmich.movielens.jobs.JobTest
import com.okmich.movielens.model.da.{Movies, Ratings}
import com.okmich.movielens.model.dw.MovieTypes
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}
import org.mockito.Mockito
import org.mockito.Mockito._


class TransFormMoviesTest  extends JobTest{

  val now: Long = Instant.now().getEpochSecond();
  implicit val movTypEncoder: Encoder[MovieTypes] = Encoders.product[MovieTypes]
  implicit val movEncoder: Encoder[Movies] = Encoders.product[Movies]


  "TransFormMovies" should " return  movie dataset by extracting the genres from Movies and Year from Ratings" in {

   val movieList: List[Movies] = List(
      Movies("1","Toy Story (1995)","Adventure|Animation|Children|Comedy|Fantasy"),
      Movies("2","Jumanji (1995)","Adventure|Children|Fantasy")
   )



    val moviesDS : Dataset[Movies] = createDataSet[Movies](movieList)


    val output : List[MovieTypes] = List(
      MovieTypes("1", "Toy Story (1995)",1995,false,true,true, true,true,false,false,false,true,false,false,false,false,false,false,false,false,false,false),
      MovieTypes("2", "Jumanji (1995)",1995,false,true,false,true,false,false,false,false,true,false,false,false,false,false,false,false,false,false,false)
    )

    val movieDF : DataFrame = createDataFrame(movieList)
    val expectedOutput : Dataset[MovieTypes] = createDataSet[MovieTypes](output)

    System.err.println("Movies DataFrame" + printDataFrame(movieDF))
    System.err.println("Movies Dataset: " + printDataSet[Movies](moviesDS))
    System.err.println("Expected Output: " + printDataSet[MovieTypes](expectedOutput))


    val job = new TransFormMovies(spark)
    val mockJob = mock(classOf[TransFormMovies],withSettings().spiedInstance(job).serializable
      .defaultAnswer(Mockito.CALLS_REAL_METHODS))
    doReturn(moviesDS).when(mockJob).readCsv[Movies]("testpath")

    val result :  Dataset[MovieTypes]  = mockJob.transformMovies(moviesDS)

    System.err.println("RESULT-DATASET: " + printDataSet[MovieTypes](result))

    result.collect should contain theSameElementsAs expectedOutput.collect

    expectedOutput should beEqualToDataSet(result)

  }
  
}

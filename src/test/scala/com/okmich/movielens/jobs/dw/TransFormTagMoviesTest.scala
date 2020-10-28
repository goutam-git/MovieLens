package com.okmich.movielens.jobs.dw

import com.okmich.movielens.jobs.JobTest
import com.okmich.movielens.model.da.{Movies, Tags}
import com.okmich.movielens.model.dw.{MovieTags, TagMovies}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}
import org.mockito.Mockito
import org.mockito.Mockito.{doReturn, mock, withSettings}

import scala.collection.mutable

class TransFormTagMoviesTest extends JobTest{
  implicit val tagsEncoder : Encoder[Tags] = Encoders.product[Tags]
  implicit val moviesEncoder: Encoder[Movies] = Encoders.product[Movies]
  implicit val tagmoviesEncoder: Encoder[TagMovies] = Encoders.product[TagMovies]

  "TransFormTagMovies" should "group the movies against individual tags" in {

    val tagsList : List[Tags] = List(
      Tags("1","1","epic","1443148538"),
      Tags("1","2","epic","1443148548"),
      Tags("1","3","thriller","1443148548")
    )

    val tagsDS: Dataset[Tags] = createDataSet[Tags](tagsList);

    System.err.println("Tags Dataset: " + printDataSet[Tags](tagsDS))

    val moviesList : List[Movies] = List(
      Movies("1","Toy Story (1995)","Adventure|Animation|Children|Comedy|Fantasy"),
      Movies("2","Jumanji (1995)","Adventure|Children|Fantasy"),
      Movies("3","Golden Eye (1995)","Adventure|Thriller")
    )
    val moviesDS : Dataset[Movies] = createDataSet[Movies](moviesList)

    System.err.println("Movies Dataset: " + printDataSet[Movies](moviesDS))


    val output : List[TagMovies] = List(
      TagMovies("thriller",Array("Golden Eye (1995)")),
      TagMovies("epic" ,Array("Toy Story (1995)","Jumanji (1995)"))

    )

    val expectedOutput : Dataset[TagMovies] = createDataSet[TagMovies](output)

    System.err.println("Expected Output: " + printDataSet[TagMovies](expectedOutput))

    val job : TransFormTagMovies = new TransFormTagMovies(spark)

    val mockJob : TransFormTagMovies  = mock(classOf[TransFormTagMovies],withSettings().spiedInstance(job)
      .serializable.defaultAnswer(Mockito.CALLS_REAL_METHODS))
    doReturn(tagsDS).when(mockJob).readCsv[Tags]("testpath")
    doReturn(moviesDS).when(mockJob).readCsv[Movies]("testpath")

    val resultDF :  DataFrame  = mockJob.transFormTagMovies(moviesDS,tagsDS)


    val result : Dataset[TagMovies] = resultDF.as[TagMovies]
    System.err.println("RESULT: " + printDataSet[TagMovies](result))
    result.collect.size  shouldEqual  expectedOutput.collect.size

    // expectedOutput should beEqualToDataSet(result)
  }

}

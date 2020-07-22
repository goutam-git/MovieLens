package com.okmich.movielens.model.da


object Movies{
  lazy val MOVIE_ID = 'movieId.name
  lazy val TITLE = 'title.name
  lazy val GENRES = 'genres.name
}
case class Movies(movieId:String,
                  title:String,
                  genres:String){
  private val SEP = "|"
  override def toString : String = "[ "+this.movieId + SEP + this.title + SEP + this.genres + " ]"
}


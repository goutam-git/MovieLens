package com.okmich.movielens.model.dw

case class RatingsMovies(movieId: Int,
                         avg_rating: Double,
                         total_rating: Double,
                         no_rating: Int,
                         first_rated_ts: Long,
                         last_rated_ts: Long) {
  private val SEP = "|"

  override def toString: String = "[ "+this.movieId + SEP + this.avg_rating + SEP + this.total_rating +
    SEP + this.no_rating +
    SEP + this.first_rated_ts +
    SEP + this.last_rated_ts + " ]"

}

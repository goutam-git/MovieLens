package com.okmich.movielens.model.dw

import java.sql.Timestamp

case class MovieRatings(userId: Int,
                        movieId: Int,
                        rating: Double,
                        year: Int,
                        month: Int,
                        dayOfMonth: Int,
                        dayOfWeek: String,
                        hour: Int,
                        minitue: Int,
                        am_pm: String,
                        ts: Long) {

  private val SEP = "|"

  override def toString: String = "[ "+this.userId + SEP + this.movieId + SEP + this.rating +
    SEP + this.year +
    SEP + this.month +
    SEP + this.dayOfMonth +
    SEP + this.dayOfWeek +
    SEP + this.hour +
    SEP + this.minitue +
    SEP + this.am_pm +
    SEP + this.ts + " ]"
}

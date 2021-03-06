package com.okmich.movielens.model.dw



case class MovieTags(userId: Int,
                movieId: Int,
                tag: String,
                year: Int,
                month: Int,
                dayOfMonth: Int,
                dayOfWeek: String,
                hour: Int,
                minitue: Int,
                am_pm: String,
                ts: Long) {

  private val SEP = "|"

  override def toString: String = "[ "+this.userId + SEP + this.movieId + SEP + this.tag +
    SEP + this.year +
    SEP + this.month +
    SEP + this.dayOfMonth +
    SEP + this.dayOfWeek +
    SEP + this.hour +
    SEP + this.minitue +
    SEP + this.am_pm +
    SEP + this.ts + " ]"

}


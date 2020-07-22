package com.okmich.movielens.model.dw

object MovieTypes extends Serializable {
  lazy val ID = 'Id.name
  lazy val TITLE = 'title.name
  lazy val YEAR = 'year.name
}

case class MovieTypes(id: String,
                 title: String,
                 year: Int,
                 isAction: Boolean,
                 isAdventure: Boolean,
                 isAnimation: Boolean,
                 isChildren: Boolean,
                 isComedy: Boolean,
                 isCrime: Boolean,
                 isDocumentary: Boolean,
                 isDrama: Boolean,
                 isFantasy: Boolean,
                 isFilmNoir: Boolean,
                 isHorror: Boolean,
                 isIMAX: Boolean,
                 isMusical: Boolean,
                 isMystery: Boolean,
                 isRomance: Boolean,
                 isScifi: Boolean,
                 isThriller: Boolean,
                 isWar: Boolean,
                 isWestern: Boolean){



  private val SEP = "|"

  override def toString: String = "[ "+this.id + SEP + this.title + SEP + this.year +
    SEP + this.isAction +
    SEP + this.isAdventure +
    SEP + this.isAnimation +
    SEP + this.isChildren +
    SEP + this.isComedy +
    SEP + this.isCrime +
    SEP + this.isDocumentary +
    SEP + this.isDrama +
    SEP + this.isFantasy +
    SEP + this.isFilmNoir +
    SEP + this.isHorror +
    SEP + this.isIMAX +
    SEP + this.isMusical +
    SEP + this.isMystery +
    SEP + this.isRomance +
    SEP + this.isScifi +
    SEP + this.isThriller +
    SEP + this.isWar +
    SEP + this.isWestern + " ]"
}

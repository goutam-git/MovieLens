package com.okmich.movielens.model.conf

object Environment {
  val fromString = Map(
    LOCAL.name -> LOCAL,
    DEV.name -> DEV,
    UACC.name -> UACC,
    INTEG.name -> INTEG,
    PROD.name -> PROD
  )

  sealed abstract class EnvironmentEnum(envName: String) {
    def name(): String = envName
  }

  case object LOCAL extends EnvironmentEnum("local")

  case object DEV extends EnvironmentEnum("dev")

  case object UACC extends EnvironmentEnum("uacc")

  case object INTEG extends EnvironmentEnum("integ")

  case object PROD extends EnvironmentEnum("prod")

}

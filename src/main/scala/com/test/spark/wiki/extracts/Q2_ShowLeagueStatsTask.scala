package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show()

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")

    standings.createOrReplaceTempView("LeagueTable")

    session.sql("SELECT season, league,avg(goalsFor) as  goalsAvg from LeagueTable group by league,season ").show()


    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    val teamTitre = standings.filter(x=> (x.position==1 && x.league.toLowerCase=="ligue 1"))
      .groupByKey(_.team).count()
      .orderBy(desc("count(1)")).first()._1


    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")

    standings.filter(x=>x.position==1).agg(avg($"points")).as[Long].show()





    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?
    val decadeval = (season: Int) => {
      season.toString.replaceAll(".$",".X")

    }
    val decade=udf(decadeval)




    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")

  }
}

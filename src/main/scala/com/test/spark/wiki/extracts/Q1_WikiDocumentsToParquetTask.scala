package com.test.spark.wiki.extracts

import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.slf4j.{Logger, LoggerFactory}
import java.io.{File, FileInputStream}
import scala.collection
import scala.collection.JavaConversions._

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDS()
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
      .flatMap {
        case (season, (league, url)) =>
          try {
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veillez à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage


            val doc =  Jsoup.connect(url).get()
             val table =    doc.select("table:has(caption:contains(Classement))")
            val ite = table.select("td").iterator
            val element=table.select("td")
            if(element.size()==10){
              LeagueStanding(league,season,position=ite.next().text().toInt,team= ite.next().text(),points=ite.next().text().toInt,played=ite.next().text().toInt,
                won=ite.next().text().toInt,drawn=ite.next().text().toInt,lost=ite.next().text().toInt,goalsFor=ite.next().text().toInt,goalsAgainst=ite.next().text().toInt,goalsDifference=ite.next().text().toInt)
            }





          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              /****
                * Spark Jobs logs:
                *
                * S3: s3://aws-logs-[accound-id]-region /elasticmapreduce//[master-id]/containers/
                *
                *
                *
                * Spark Server:
                *
                * EMR Master node: /var/log/spark/spark-history-server.out
                *
                * S3: s3://aws-logs-[accound-id]-region/elasticmapreduce/[master-id]/node/[ec2-id]/applications/spark/spark-history-server.out.gz
                *
                *
                *
                * YARN :
                *
                * EMR Master node: /mnt/var/log/hadoop-yarn
                *
                * S3: s3://aws-logs-736182580110-eu-west-1/elasticmapreduce/[master-id]/node/[ec2-id /applications/hadoop-yarn/
                */
              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
      }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    /*
    Une sequence en Scala est une structure de données classique qui permet de stocker des objets dans une liste ordonnée
et qui offre des opérations d'ajout, suppression,..
D'autre part une API Dataset permet de:
-interroger  les données en utilisant SQL
-traiter les données d'une façons distribués et paralléle

     */
  }


  d

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val yamlfile = getClass.getResource("/leagues.yaml").getPath
    val input = new FileInputStream(new File(yamlfile))
      mapper.readValue(input, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name") name: String,
@JsonProperty ("url") url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)

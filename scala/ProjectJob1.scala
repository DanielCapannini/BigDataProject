package lab104

import lab104.ProjectJob2.{path_name_basic, path_title_basic, path_title_principals, path_title_ratings}
import org.apache.spark.sql.SparkSession
import utils._
import org.apache.spark.sql.SaveMode
import org.apache.spark.HashPartitioner
import org.apache.spark.sql._
import utils.Commons


object ProjectJob1 {
  val path_to_datasets = "/project/"
  val path_name_basic = path_to_datasets + "name.basics.csv"
  val path_title_basic = path_to_datasets + "title.basics.csv"
  val path_title_principals = path_to_datasets + "title.principals.csv"
  val path_title_ratings = path_to_datasets + "title.ratings.csv"
  val path_output = "/output/project/job1"

  def main(args: Array[String]): Unit = {
    import org.apache.spark.HashPartitioner
    val spark = SparkSession.builder.appName("project job 1").getOrCreate()
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._


    val rddTitleBasics = spark.sparkContext.textFile(Commons.getDatasetPath("sharedRemote", path_title_basic)).flatMap(IMDbParser.parseTitleBasicsLine)
    val rddPrincipals = spark.sparkContext.textFile(Commons.getDatasetPath("sharedRemote", path_title_principals)).flatMap(IMDbParser.parsePrincipalsLine)
    val rddRatings = spark.sparkContext.textFile(Commons.getDatasetPath("sharedRemote", path_title_ratings)).flatMap(IMDbParser.parseRatingsLine)
    val rddNameBasics = spark.sparkContext.textFile(Commons.getDatasetPath("sharedRemote", path_name_basic)).flatMap(IMDbParser.parseNameBasicsLine)

    val rddMovie = rddTitleBasics.filter(_._2 == "movie").flatMap {
      case (tconst, _, _, _, _, _, _, _, genres) => genres.map(genre => (tconst, genre))}

    val actorsAndActresses = rddNameBasics.filter { case (_, _, _, _, primaryProfession, _) =>
      primaryProfession.contains("actor") || primaryProfession.contains("actress")
    }.filter(_._4 == 0).map { case (nconst, primaryName, _, _, _, _) => (nconst, primaryName) }

    val broadcastRating = spark.sparkContext.broadcast(rddRatings.filter(_._3 > 500).map { case (tconst, rating, _) => (tconst, rating) }.collectAsMap())

    val moviesRating = spark.sparkContext.broadcast(rddPrincipals.filter { case (_, _, _, primaryProfession, _, _) =>
        primaryProfession.contains("actor") || primaryProfession.contains("actress")
      }.map { case (tconst, _, nconst, category, _, _) => (tconst, nconst) }
      .flatMap { case (tconst, nconst) =>
        broadcastRating.value.get(tconst) match {
          case Some(rating) => Some(tconst, (nconst, rating))
          case None => None
        }
      }.collectAsMap())

    val actorGenreRatings = spark.sparkContext.broadcast(rddMovie.flatMap { case (tconst, genre) =>
      moviesRating.value.get(tconst) match {
        case Some((nconst, rating)) => Some((nconst, genre), rating)
        case None => None
      }
    }.aggregateByKey((0.0, 0))(
      (acc, rating) => (acc._1 + rating, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map{
      case ((nconst, genre), (sum, count)) =>
        val sufficiency = if (sum / count > 6 && count >= 2) "sufficiente" else "insufficiente"
        ((nconst, genre), sufficiency)
    }.collectAsMap())

    val broadcastGenres = spark.sparkContext.broadcast(rddTitleBasics.filter(_._2 == "movie").flatMap(_._9).filter(x => x!= "\\N").distinct().collect())

    actorsAndActresses.flatMap { case (nconst, primaryName) =>
        broadcastGenres.value.map(genre => ((nconst, genre), primaryName))
      }.map{ case ((nconst, genre), primaryName) =>
        actorGenreRatings.value.get((nconst, genre)) match {
          case Some(sufficiency) => (nconst, primaryName, genre, sufficiency)
          case None => (nconst, primaryName, genre, "inclassificabile")
        }
      }.coalesce(1)
      .toDF().write.format("csv").mode(SaveMode.Overwrite)
      .save(Commons.getDatasetPath("remote", path_output))
  }
}

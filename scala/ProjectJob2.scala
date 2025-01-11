package lab104

import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Commons

object ProjectJob2{
  val path_to_datasets = "/project/"
  val path_name_basic = path_to_datasets + "name.basics.csv"
  val path_title_basic = path_to_datasets + "title.basics.csv"
  val path_title_principals = path_to_datasets + "title.principals.csv"
  val path_title_ratings = path_to_datasets + "title.ratings.csv"
  val path_output = "/output/project/job2"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("project job 2").getOrCreate()
    val sqlContext = spark.sqlContext // needed to save as CSV
    import sqlContext.implicits._

    val rddTitleBasics = spark.sparkContext.textFile(Commons.getDatasetPath("sharedRemote", path_title_basic)).flatMap(IMDbParser.parseTitleBasicsLine)
    val rddPrincipals = spark.sparkContext.textFile(Commons.getDatasetPath("sharedRemote", path_title_principals)).flatMap(IMDbParser.parsePrincipalsLine)
    val rddRatings = spark.sparkContext.textFile(Commons.getDatasetPath("sharedRemote", path_title_ratings)).flatMap(IMDbParser.parseRatingsLine)
    val rddNameBasics = spark.sparkContext.textFile(Commons.getDatasetPath("sharedRemote", path_name_basic)).flatMap(IMDbParser.parseNameBasicsLine)

    import org.apache.spark.HashPartitioner

    val p = new HashPartitioner(20)

    val distinctGenres = rddTitleBasics.filter(_._2 == "movie").flatMap(_._9).filter(x => x!= "\\N").distinct().collect()

    val actorsAndActresses = rddNameBasics.filter { case (_, _, _, _, primaryProfession, _) =>
      primaryProfession.contains("actor") || primaryProfession.contains("actress")
    }.filter(_._4 == 0).map { case (nconst, primaryName, _, _, _, _) => (nconst, primaryName) }

    val moviesRating = rddPrincipals.filter { case (_, _, _, primaryProfession, _, _) =>
        primaryProfession.contains("actor") || primaryProfession.contains("actress")
      }.map { case (tconst, _, nconst, category, _, _) => (tconst, nconst) }
      .join(rddRatings.filter(_._3 > 500).map { case (tconst, rating, _) => (tconst, rating) }.partitionBy(p))

    val actorGenreRatings = rddTitleBasics.filter(_._2 == "movie").flatMap {
        case (tconst, _, _, _, _, _, _, _, genres) => genres.map(genre => (tconst, genre))
      }.join(moviesRating.partitionBy(p))
      .map { case (tconst, (genre, (nconst, rating))) => ((nconst, genre), rating) }
      .groupByKey()
      .mapValues(ratings => (ratings.sum / ratings.size, ratings.size))

    actorsAndActresses.flatMap { case (nconst, primaryName) =>
        distinctGenres.map(genre => (nconst, primaryName, genre))
      }.map { case (nconst, primaryName, genre) =>
        ((nconst, genre), (primaryName, genre))
      }.leftOuterJoin(actorGenreRatings)
      .map { case ((nconst, genre), ((primaryName, _), maybeRatings)) =>
        val sufficiency = maybeRatings match {
          case Some((avgRating, count)) =>
            if (avgRating > 6 && count >= 2) "sufficiente" else "insufficiente"
          case None => "inclassificabile"
        }
        (nconst, primaryName, genre, sufficiency)
      }.coalesce(1)
      .toDF().write.format("csv").mode(SaveMode.Overwrite)
      .save(Commons.getDatasetPath("remote",path_output))

  }
}

package lab104

object IMDbParser {

  def parsePrincipalsLine(line: String): Option[(String, Int, String, String, String, String)] = {
    val commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    val input = line.split(commaRegex).map(_.trim.replaceAll("^\"|\"$", ""))
    try {
      Some(
        input(0), // tconst
        input(1).toInt, // ordering
        input(2), // nconst
        input(3), // category
        if (input(4) == "\\N") "" else input(4), // job
        if (input(5) == "\\N") "" else input(5) // characters
      )
    } catch {
      case e: Exception => None
    }
  }

  def parseRatingsLine(line: String): Option[(String, Double, Int)] = {
    val commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    val input = line.split(commaRegex).map(_.trim.replaceAll("^\"|\"$", ""))
    try {
      Some(
        input(0), // tconst
        input(1).toDouble, // averageRating
        input(2).toInt // numVotes
      )
    } catch {
      case e: Exception => None
    }
  }

  def parseNameBasicsLine(line: String): Option[(String, String, Int, Int, Array[String], Array[String])] = {
    val commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    val input = line.split(commaRegex).map(_.trim.replaceAll("^\"|\"$", ""))
    try {
      Some(
        input(0), // nconst
        input(1), // primaryName
        input(2).toInt, // birthYear
        if (input(3) == "\\N") 0 else input(3).toInt, // deathYear
        input(4).split(",").map(_.trim), // primaryProfession as Array[String]
        input(5).split(",").map(_.trim) // knownForTitles as Array[String]
      )
    } catch {
      case e: Exception => None
    }
  }

  def parseTitleBasicsLine(line: String): Option[(String, String, String, String, Boolean, Int, Int, Int, Array[String])] = {
    val commaRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    val input = line.split(commaRegex).map(_.trim.replaceAll("^\"|\"$", ""))
    try {
      Some(
        input(0), // tconst
        input(1), // titleType
        input(2), // primaryTitle
        input(3), // originalTitle
        input(4) == "1", // isAdult
        input(5).toInt, // startYear
        if (input(6) == "\\N") 0 else input(6).toInt, // endYear
        if (input(7) == "\\N") 0 else input(7).toInt, // runtimeMinutes
        input(8).split(",").map(_.trim) // genres as Array[String]
      )
    } catch {
      case e: Exception => None
    }
  }
}

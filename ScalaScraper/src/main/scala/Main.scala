import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.parallel.mutable.ParArray
import scala.collection.parallel.TaskSupport
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Try

import java.io._
import scala.collection.JavaConversions._

import scala.xml.Node
import scala.xml.NodeSeq

import io.circe.generic.JsonCodec, io.circe.syntax._
import io.circe.parser.decode

import com.github.tototoshi.csv._

object Main {
  // Make sure these folders exist
  var MONTHLY_JSON_FOLDER = "monthlyJson"
  var XML_TOP_FOLDER = "data"
  var THREADS: Int = 2
  var NORMALIZE = true

   def main(args: Array[String]) = {
    if (args.length >= 3) {
      if (args(0) == "createJson") {
        XML_TOP_FOLDER = args(1)
        MONTHLY_JSON_FOLDER = args(2)
        if (args.length == 4) THREADS = args(3).toInt
        createMonthlyJsons
      } else if (args(0) == "createCSV") {
        MONTHLY_JSON_FOLDER = args(1)
        NORMALIZE = args(3).toBoolean
        createCSV(args(2))
      }
    } else if (args(0) == "fromCode") {
      createMonthlyJsons
      createCSV("countries")
    } else {
      println("I do not know what to do")
    }
  }

  // Circe semi-automatic derivation <3
  @JsonCodec case class ArticleData(date: (Int, Int), word_count: Int, page_no: Option[Int],
                                    counts: HashMap[String, HashMap[String, Int]], title_counts: HashMap[String, HashMap[String, Int]])

  @JsonCodec case class MonthArticles(journal: String, month: Int, year: Int, articles: Seq[ArticleData]) {
    val filename: String = s"$MONTHLY_JSON_FOLDER/$journal $month-$year.json"

    def dumpToFile = writeJsonToFile(this.asJson, filename)
  }

 /*
   * Contains the words that we want to count
   * Example {countries: {Italie: 10, France: 30}, Cities: {New York: 2}}
   */
  val words = decode[HashMap[String, Seq[String]]](Source.fromFile("words.json").getLines().mkString).toOption match {
    case Some(ws) => ws
    case None => throw new IllegalStateException("Could not read data file")
  }

  /**
    * Creates a summary CSV file for a category (one row per (month, year), one column per word in the category)
    */
  def createCSV(category: String) = {
    val counts = countCategory(category)

    val sorted: Stream[((Int, Int), HashMap[String, Double])] = counts.toStream.sortBy(_._1)
    val wordList: Seq[String] = words.getOrElse(category, List())
    val writer = CSVWriter.open(new File(s"$category-$NORMALIZE.csv"))

    writer.writeRow(List("month", "year") ++: wordList)

    sorted foreach { case (date, counts) => writer.writeRow(date._1 +: (date._2 +: wordList.map(w => counts.getOrElse(w, 0)))) }

    writer.close()
  }

   /**
    * Aggregates all the counts for one category (e.g countries) by grouping them by (month, year) and then
    * summing the values of each group
    */
  def countCategory(category: String): HashMap[(Int, Int), HashMap[String, Double]] = {
    val jsons: Stream[File] = getRecursiveListOfFiles(new File(MONTHLY_JSON_FOLDER)).filter(!_.isDirectory)
    val res = new HashMap[(Int, Int), HashMap[String, Double]]()
    for {
      file <- jsons
      monthData <- decode[MonthArticles](Source.fromFile(file).getLines().mkString).toOption
      art <- monthData.articles
    } {
      // Always defined (see filter above)
      val countsForDate = res.getOrElse(art.date, new HashMap[String, Double])
      val countsForArticle = art.counts.getOrElse(category, new HashMap[String, Int])
      for ((k, v) <- countsForArticle) {
        val increment: Double = if (NORMALIZE) v else v / art.word_count.toDouble
        val old: Double = countsForDate.getOrElse(k, 0)
        countsForDate.put(k, old + increment)
      }
      if (!res.contains(art.date) && !countsForDate.isEmpty) res.put(art.date, countsForDate)
    }
    res
  }

  def monthYear(file: File): (Int, Int) = {
    val fullPath = file.getAbsolutePath()
    val words: Array[String] = fullPath.dropRight(4).split("/")
    val month = words(words.length - 1)
    val year = words(words.length - 2)

    (month.toInt, year.toInt)
  }

  /**
    * Scans XML data in XML_TOP_FOLDER and creates temporary jsons into MONTHLY_JSON_FOLDER
    */
  def createMonthlyJsons() = {
    val files: ParArray[File] = getRecursiveListOfFiles(new File(XML_TOP_FOLDER)).filter(!_.isDirectory).toParArray
    files.tasksupport = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(THREADS))

    for (file <- files; art <- scanXML(xml.XML.loadFile(file), monthYear(file))) {
      art.dumpToFile
      println("Written " + art.filename)
    }
    /*
    // This commented code looks better BUT foreach sur le stream memoizes the content of
    // the stream (e.g. it tries to memorize all the 18 gigs of XML file)
    // The solution is the ugly for loop above or ScalaZ EphemeralStreams
    val xmlFiles: Stream[Node] = getRecursiveListOfFiles(new File(s"$XML_TOP_FOLDER")).filter(!_.isDirectory).map(xml.XML.loadFile)
    val ms: Stream[MonthArticles] = xmlFiles.flatMap(scanXML)

    ms.foreach(m => {
        m.dumpToFile
        println("Written " + m.filename)
    })
    */
  }

  /**
    * Receives an XML document, counts the words and retrieves some metadata
    */
  def scanXML(doc: Node, monthYear: (Int, Int)): Option[MonthArticles] = {
    val (month, year) = monthYear
    val arts = (doc \\ "article").flatMap(art => processArticle(art, monthYear))

    if (!arts.isEmpty) {
      val meta = doc \\ "monthEntity"

      val journal = meta \@ "issue"
      Some(MonthArticles(journal, month, year, arts))
    } else {
      None
    }
  }

  /**
    * Processes one article extracting Title, Date, Word count, page number,
    * counts of words in text and tile
    */
  def processArticle(article: Node, monthYear: (Int, Int)): Option[ArticleData] = {
    val meta: NodeSeq = article \\ "meta"

    val title = (meta \ "name").text
    val page_no: Option[Int] = Try((meta \ "page_no").text.toInt).toOption

    val counts = HashMap[String, HashMap[String, Int]]()
    val title_counts = HashMap[String, HashMap[String, Int]]()

    val text = (article \\ "full_text").text
    val word_count: Int = text.split(' ').length//Try((meta \ "word_count").text.toInt).toOption
    for ((category, listOfWords) <- words) {
      val categoryData = counts.getOrElse(category, new HashMap[String, Int]())
      val titleCategoryData = title_counts.getOrElse(category, new HashMap[String, Int]())

      for (word <- listOfWords) {
        if (text.contains(word)) categoryData.update(word, categoryData.getOrElse(word, 0) + 1)
        if (title.contains(word)) titleCategoryData.update(word, titleCategoryData.getOrElse(word, 0) + 1)
      }

      if (!counts.contains(category) && !categoryData.isEmpty) counts.put(category, categoryData)
      if (!title_counts.contains(category) && !titleCategoryData.isEmpty) title_counts.put(category, titleCategoryData)
    }

    if (!counts.isEmpty || !title_counts.isEmpty) Some(ArticleData(monthYear, word_count, page_no, counts, title_counts))
    else None
  }

  def writeJsonToFile(o: io.circe.Json, filename: String) {
    val pw = new PrintWriter(new File(filename))
    pw.write(o.noSpaces)
    pw.close
  }

  def getRecursiveListOfFiles(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getRecursiveListOfFiles) else Stream.empty)

  implicit val dateOrdering: math.Ordering[(Int, Int)] = new Ordering[(Int, Int)] {
    override def compare(x: (Int, Int), y: (Int, Int)): Int = {
      val ((monthA, yearA), (monthB, yearB)) = (x, y)
      val yearDiff = yearA - yearB
      if (yearDiff != 0) yearDiff else monthA - monthB
    }
  }
}

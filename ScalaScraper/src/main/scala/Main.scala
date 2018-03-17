import scala.io.Source
import scala.collection.mutable.HashMap
import scala.util.Try

import java.io._
import scala.collection.JavaConversions._

import scala.xml.Node
import scala.xml.NodeSeq

import io.circe.generic.JsonCodec, io.circe.syntax._
import io.circe.parser.decode

import com.github.tototoshi.csv._

object Main {
    def main(args: Array[String]) = {
        if (args.length == 3) {
            if (args(0) == "createJson") {
                XML_TOP_FOLDER = args(1)
                MONTHLY_JSON_FOLDER = args(2)
                createMonthlyJsons
            } else if (args(0) == "createCSV") {
                MONTHLY_JSON_FOLDER = args(1)
                createCSV(args(2))
            }
        } else if (args(0) == "fromCode") {
            createMonthlyJsons
            createCSV("countries")
        } else {
            println("I do not know what to do")
        }
    }

    // Make sure these folders exist
    var MONTHLY_JSON_FOLDER = "monthlyJson"
    var XML_TOP_FOLDER = "data"

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

        val sorted: Stream[((Int, Int), HashMap[String, Int])] = counts.toStream.sortBy(_._1)(dateOrdering)
        val wordList: Seq[String] = words.getOrElse(category, List())
        val writer = CSVWriter.open(new File(s"$category.csv"))

        writer.writeRow(List("month", "year") ++: wordList)

        sorted foreach {case (date, counts) => writer.writeRow(date._1 +: (date._2 +: wordList.map(w => counts.getOrElse(w, 0))))}

        writer.close()
    }

    /**
     * Aggregates all the counts for one category (e.g countries) by grouping them by (month, year) and then
     * summing the values of each group
     */
    def countCategory(category: String): HashMap[(Int, Int), HashMap[String, Int]] = {
        val jsons: Stream[File] = getRecursiveListOfFiles(new File(s"$MONTHLY_JSON_FOLDER")).filter(!_.isDirectory)
        val months: Stream[MonthArticles] = jsons.map(j => Source.fromFile(j).getLines().mkString).flatMap(decode[MonthArticles](_).toOption)
        val articles: Stream[ArticleData] = months.flatMap(ma => ma.articles).filter(_.date.isDefined)

        val res = new HashMap[(Int, Int), HashMap[String, Int]]()
        articles.foreach(art => {
            // Always defined (see filter above)
            val tmpDate: Array[Int] = art.date.getOrElse(Array(0, 0, 0))

            val date = (tmpDate(1), tmpDate(2))
            val countsForDate = res.getOrElse(date, new HashMap[String, Int])
            val countsForArticle = art.counts.getOrElse(category, new HashMap[String, Int])
            mergeIntoFirst(countsForDate, countsForArticle)

            if (!res.contains(date) && !countsForDate.isEmpty) res.put(date, countsForDate)
        })
        res
    }

    /**
     * Scans XML data in XML_TOP_FOLDER and creates temporary jsons into MONTHLY_JSON_FOLDER
     */
    def createMonthlyJsons() = {
        val xmlFiles: Stream[Node] = getRecursiveListOfFiles(new File(s"$XML_TOP_FOLDER")).filter(!_.isDirectory).map(xml.XML.loadFile)
        val ms: Stream[MonthArticles] = xmlFiles.flatMap(scanXML)

        ms.foreach(m => {
            m.dumpToFile
            println("Written " + m.filename)
        })
    }

    // Circe semi-automatic derivation <3
    @JsonCodec case class ArticleData(date: Option[Array[Int]], word_count: Option[Int], page_no: Option[Int],
                                      counts: HashMap[String, HashMap[String, Int]], title_counts: HashMap[String, HashMap[String, Int]])
    @JsonCodec case class MonthArticles(journal: String, month: String, year: String, articles: Seq[ArticleData]) {
        val filename: String = s"$MONTHLY_JSON_FOLDER/$journal $month-$year.json"
        def dumpToFile = writeJsonToFile(this.asJson, filename)
    }

    /**
     * Receives an XML document, counts the words and retrieves some metadata
    */
    def scanXML(doc: Node): Option[MonthArticles] = {
        val arts = (doc \\ "article").flatMap(processArticle)

        if (!arts.isEmpty) {
            val meta = doc \\ "monthEntity"

            val journal = meta \@ "issue"
            val month = meta \@ "month"
            val year = meta \@ "year"

            Some(MonthArticles(journal, month, year, arts))
        } else {
            None
        }
    }

    /**
     * Processes one article extracting Title, Date, Word count, page number,
     * counts of words in text and tile
     */
    def processArticle(article: Node): Option[ArticleData] = {
        val meta: NodeSeq = article \\ "meta" 

        // We study a time series, we have no need for untimed data
        val dateString = (meta \ "issue_date").text
        if (dateString.size < 10) {
            println("Invalid date")
            return None
        }

        val date = dateString.substring(0, 10).replace("/", "-")

        val title = (meta \ "name").text
        val page_no = {
            try {Some((meta \ "page_no").text.toInt)}
            catch {case e : java.lang.NumberFormatException => None}
        }
        val word_count = {
            try {Some((meta \ "word_count").text.toInt)}
            catch {case e: java.lang.NumberFormatException => None}
        }

        val counts = HashMap[String, HashMap[String, Int]]()
        val title_counts = HashMap[String, HashMap[String, Int]]()

        val text = (article \\ "full_text").text
        for ((category, listOfWords) <- words) {
            val categoryData = counts.getOrElse(category, new HashMap[String, Int]())
            val titleCategoryData = title_counts.getOrElse(category, new HashMap[String, Int]())

            for (word <- listOfWords) {
                if (text.contains(word)) categoryData.update(word, categoryData.getOrElse(word, 0) + 1)
                if (title.contains(word)) titleCategoryData.update(word, categoryData.getOrElse(word, 0) + 1)
            }

            if (!counts.contains(category) && !categoryData.isEmpty) counts.put(category, categoryData)
            if (!title_counts.contains(category) && !titleCategoryData.isEmpty) title_counts.put(category, titleCategoryData)
        }

        if (!counts.isEmpty || !title_counts.isEmpty) Some(ArticleData(parseDate(date), word_count, page_no, counts, title_counts))
        else None
    }

    def parseDate(s: String): Option[Array[Int]] = {
        try {Some(s.split("-").map(_.toInt))}
        catch {case e : java.lang.NumberFormatException => None}
    }
 
    def writeJsonToFile(o: io.circe.Json, filename: String) {
        val pw = new PrintWriter(new File(filename))
        pw.write(o.noSpaces)
        pw.close
    }

    def getRecursiveListOfFiles(f: File): Stream[File] =
        f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getRecursiveListOfFiles) else Stream.empty)

    def mergeIntoFirst(left: HashMap[String, Int], right: HashMap[String, Int]): Unit = {
        for ((k, v) <- right) left.put(k, left.getOrElse(k, 0) + v)
    }

    val dateOrdering: math.Ordering[(Int, Int)] = new Ordering[(Int, Int)] {
        override def compare(x: (Int, Int), y: (Int, Int)): Int = {
            val ((monthA, yearA), (monthB, yearB)) = (x, y)
            val yearDiff = yearA - yearB
            if (yearDiff != 0) yearDiff else monthA - monthB
        }
    }
}

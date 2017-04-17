
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.io.Source
import scala.util.parsing.json.JSON
import scala.collection.mutable.ListBuffer
import java.nio.charset.StandardCharsets

object Sentiment {

  def parseLine(jsonItem: String): Array[String] = {
    val result = JSON.parseFull(jsonItem).get.asInstanceOf[Map[String,String]]
    val tweetText = result("text").toLowerCase
    val temp = tweetText.getBytes(StandardCharsets.UTF_8)
    val ConvertedTweet  = new String(temp, StandardCharsets.UTF_8)
    ConvertedTweet.split ("[^A-Za-z]")
  }

  def splitToWord(index: Long, words: Array[String]): List[(Long , String)] ={
    var wordListBuffer = new ListBuffer[(Long, String)]()
    for (x <- words) {
      var wordTuple = (index ,x)
      wordListBuffer += wordTuple
    }
    val wordList = wordListBuffer.toList
    wordList
  }

  def getScore(word: String, aff: scala.collection.mutable.HashMap[String, Int]): Int = {
    if (aff.contains(word))
      aff(word)
    else
      0
  }

  def main(args: Array[String]) {
    val aff = new scala.collection.mutable.HashMap[String, Int]
    for (line <- Source.fromFile(args (1)).getLines ) {
      val words = line.split("""\t""")
      aff.put (words (0), words (1).toInt)
    }

    val sparkConf = new SparkConf().setAppName ("Spark Sentiment").setMaster("local[1]")
    val sc = new SparkContext (sparkConf)
    val text = sc.textFile (args (0) )
    val counts = text.zipWithIndex().map({case(line, i) => (i+1, Sentiment.parseLine(line))})
      .flatMap({case(i, words) => splitToWord(i, words)})
      .map(tuple => (tuple._1, getScore(tuple._2, aff))).reduceByKey(_+_).sortByKey(true)
    counts.saveAsTextFile("weiwei")
  }

}

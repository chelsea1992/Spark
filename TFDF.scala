import java.nio.charset.StandardCharsets
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer


object TFDF {

  def parseLineTest(jsonItem: String): Array[String] = {
    val result = JSON.parseFull(jsonItem).get.asInstanceOf[Map[String,String]]
    val tweetText = result("text").toLowerCase
    val temp = tweetText.getBytes(StandardCharsets.UTF_8)
    val ConvertedTweet  = new String(temp, StandardCharsets.UTF_8)
    ConvertedTweet.replaceAll("[\\-#@]", "").replaceAll("""[\p{Punct}]"""," ")
      .split("\\s+")
  }

  def splitToWordAndCount(index: Long, words: Array[String]): List [(String, (Long, Int))] ={
    val wordMap = new scala.collection.mutable.HashMap[(Long, String), Int]
    var wordAndCountListBuffer = new ListBuffer[(String,  (Long, Int))]()
    for (x <- words) {
      if (wordMap.contains((index , x))){
        wordMap(index , x) += 1 } else{
        wordMap += (index , x ) -> 1
      }
    }
    wordMap.foreach {case((index, word), count) => wordAndCountListBuffer += ((word, (index, count)))}
    wordAndCountListBuffer.toList
  }


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName ("Spark TFDF").setMaster("local[1]")
    val sc = new SparkContext (sparkConf)
    val text = sc.textFile (args (0) )
    val counts = text.zipWithIndex().map({case(line, i) => (i+1, parseLineTest(line))})
      .flatMap({case(i, words) => splitToWordAndCount(i, words)}).groupByKey().sortByKey(true).mapValues(_.toList)
      .map({case(word, list) => (word, list.length, list)})
    counts.saveAsTextFile("wei_wei")
  }
}

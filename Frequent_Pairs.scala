import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex


object spark {

  def parseline(line: String): (String,String) =
  {
    var temp = new Array[String](2)
    var parsedLineArray = new Array[String](2)
    var tuple1 = ""
    var tuple2 = ""
    //val pattern = new Regex("""^\((['"])(.*)\1, (['"])(.*)\3\)$""")
    val pattern = new Regex("""^\(((['"])(.*)\2), ((['"])(.*)\5)\)$""")
    line match {
      case pattern(movie, q1, q2, personName,q3,q4) => {
        tuple1 = movie
        tuple2 = personName
      }
    }
    val parsedLineTuple = tuple1 -> tuple2

    parsedLineTuple
  }

  def filterFun(number:Int,numberB:Int): Boolean ={
    if (number>=numberB)return true
    else return false
  }

  def combine(listA:List[List[String]]): List[(String,String)]={
    var listB = new ListBuffer[(String,String)]()
    for(a <- listA){
      for (comb <- a.combinations(2)){listB += Tuple2(comb(0)," "+ comb(1)) }
    }
    if(listA.size ==2) {
      for (i <- listA(0)) {
        for (j <- listA(1))
          listB += Tuple2(i," " +j)
      }
    }
    listB.toList
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark Sentiment").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    val directorRDD = sc.textFile(args(1))
    val actressRDD = sc.textFile(args(0))
    val supportNum = (args(2)).toInt
    val resultDirector = directorRDD.map(line => parseline(line)).groupByKey().map({case(k,l1:Iterable[String]) =>(k,l1.toList)})
    val resultActress = actressRDD.map(line => parseline(line)).groupByKey().map({case(k,l2:Iterable[String]) =>(k,l2.toList)})
    val joinRDD = resultActress.union(resultDirector).groupByKey().map({ case (i, l3: Iterable[List[String]]) => (i, l3.toList)}).
      flatMap({case(i,listB)=>combine(listB)}).map({case(a,b) =>(a,b) -> 1}).reduceByKey(_+_).sortByKey(true).filter({case((x,y), num) => filterFun(num,supportNum)}).sortBy(_._2).
      map({case((x1,y1), num) => ((x1,y1), " " +num)})
    joinRDD.coalesce(1).saveAsTextFile("Qiaozhi_Song_spark")
    }
}





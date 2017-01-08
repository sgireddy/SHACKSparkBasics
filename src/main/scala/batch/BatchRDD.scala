package batch

import domain.Activity
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.apache.spark.{SparkConf, SparkContext}
import scala.util._
import util._
/** Created by Shashi Gireddy (https://github.com/sgireddy) on 1/2/17 */
object BatchRDD extends App {
  val conf = new SparkConf().setAppName("Lambda with Spark").setMaster("local[*]")
  val sc = new SparkContext(conf)
  implicit val formats = DefaultFormats
  val sourceFile = "/home/sri/tmp/data.txt"
  val input = sc.textFile(sourceFile)

  //SG: Optimistic approach, we get json data which is being statically bound to Activity
  //  val inputRDD = input.flatMap { line => Some(parse(line).extract[Activity]) }

  //SG: Discard invalid lines
  //  val inputRDD = input.flatMap { line =>
  //    Try(parse(line)).toOption.flatMap( jv => Try(jv.extract[Activity]).toOption.flatMap( v => Some(v)) )
  //  }

  //SG: Discard invalid lines Make is readable using for comprehensions instead of flatmaps
  //  val inputRDD = for {
  //    line <- input
  //    jv <- Try(parse(line)).toOption
  //    act <- Try(jv.extract[Activity]).toOption
  //  } yield act

  //SG: Best of all, for comprehensions with error handling
  val inputRDD = for {
    line <- input
    jv <- errorHandler(Try(parse(line)))
    act <- errorHandler(Try(jv.extract[Activity]))
  } yield act

  val keyedByProduct = inputRDD.keyBy( a => a.productId).cache()
  println(keyedByProduct.count())
  val visitorsByProduct = keyedByProduct.mapValues( a => (a.userId)).distinct().countByKey()
  val sample = visitorsByProduct.take(100)
  println(sample)
  /****** Do more stuff ****/
}
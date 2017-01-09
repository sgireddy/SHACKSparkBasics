package batch
import config.Settings
import domain.Activity
import org.apache.spark.{SparkConf, SparkContext}
import util._
/** Created by Shashi Gireddy (https://github.com/sgireddy) on 1/2/17 */
object BatchRDD extends App {
  import Settings.ClickStreamGeneratorSettings._
  val conf = new SparkConf().setAppName(appName).setMaster(s"${sparkMaster}[${numCores}]")
  val sc = new SparkContext(conf)
  val sourceFile = tmpFile
  val input = sc.textFile(sourceFile)

  //SG: Optimistic approach, we get json data which is being statically bound to Activity
  //  val inputRDD = input.flatMap { line => Some(parse(line).extract[Activity]) }

  //SG: Discard invalid lines
  //  val inputRDD = input.flatMap { line =>
  //    Try(parse(line)).toOption.flatMap( jv => Try(jv.extract[Activity]).toOption.flatMap( v => Some(v)) )
  //  }

  //SG: Discard invalid lines Make it readable using for-comprehensions instead of flatmaps
//    val inputRDD = for {
//      line <- input
//      jv <- Try(parse(line)).toOption
//      act <- Try(jv.extract[Activity]).toOption
//    } yield act

  //SG: Best of all, for-comprehensions with errorHandler from our util package
  //  val inputRDD = for {
  //    line <- input
  //    jv <- errorHandler(Try(parse(line)))
  //    act <- errorHandler(Try(jv.extract[Activity]))
  //  } yield act

  //SG: Push generic logic to util package,  json parsing & error handling
  val inputRDD = for {
    line <- input
    activity <- tryParse[Activity](line)
  } yield activity


  val keyedByProduct = inputRDD.keyBy( a => a.productId).cache()
  println(keyedByProduct.count())
  val visitorsByProduct = keyedByProduct.mapValues( a => (a.userId)).distinct().countByKey()
  val sample = visitorsByProduct.take(100)
  println(sample)
  /****** Do more stuff ****/
}
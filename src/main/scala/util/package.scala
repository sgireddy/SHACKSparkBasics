import net.liftweb.json.{DefaultFormats, parse}
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}

/** Created by Shashi Gireddy (https://github.com/sgireddy) on 1/2/17 */
package object util {
  val rnd = new Random()
  def getProducts(): scala.collection.Map[Int, Int] ={
    var products = scala.collection.mutable.Map[Int, Int]()
    for(prd <- 1000 to 10000) {
      prd match {
        case prd if prd % 5 > 0 => products += ((prd, rnd.nextInt(100) + 1))
        case prd if prd % 3 > 0 => products += ((prd, rnd.nextInt(50) + 1))
        case prd if prd % 2 > 0 => products += ((prd, rnd.nextInt(20) + 1))
        case _ => products += ((prd, rnd.nextInt(10) + 1))
      }
    }
    products.toMap
  }

  def getProductDiscount(productId: Int): Int = {
    productId match {
      case productId if productId % 7 == 0 => 25
      case productId if productId % 5 == 0 => 20
      case productId if productId % 3 == 0 => 10
      case _ => 0
    }
  }

  def getProductMargin(productId: Int): Int = {
    productId match {
      case productId if productId % 2 == 0 => 30
      case productId if productId % 5 == 0 => 25
      case productId if productId % 3 == 0 => 15
      case _ => 10
    }
  }

  def getCartDiscount(productId: Int): Int = {
    productId match {
      case productId if productId % 5 == 0 => 10
      case _ => rnd.nextInt(2) * 5
    }
  }
  
  def getRandomReferrer(): String = {
    val list = List("google", "facebook", "bing", "yahoo", "site")
    list(rnd.nextInt(5))
  }

  def getAction(prodDisc: Int = 0, cartDisc: Int = 0): Int = {
    (prodDisc, cartDisc ) match {
      case (x, y) if x+y > 25 => 2
      case _ => rnd.nextInt(3)
    }
  }

  def errorHandler[T](tv: Try[T]) : Option[T] = {
    tv match {
      case Success(v) => Some(v)
      case Failure(ex) => {
        println(s"${ex.getMessage} ${System.lineSeparator()} ${ex.getStackTrace}")
        None
      }
    }
  }

  def tryParse[T](line: String ) (implicit m: TypeTag[T]) : Option[T] = {
    implicit val formats = DefaultFormats
    implicit val cl = ClassTag[T](m.mirror.runtimeClass(m.tpe))
    for {
      jv <- errorHandler(Try(parse(line)))
      v <- errorHandler(Try(jv.extract[T]))
    } yield v
  }
}
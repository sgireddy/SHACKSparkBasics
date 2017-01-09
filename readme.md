<h1>Let's build a SOLID SHACK and scale it</h1>

This is an attempt to promote SOLID SHACK architecture. Let's build a SOLID SHACK and scale it to enterprise level.
This work is a part of multi series blog starting with Spark Fundamentals. 

SHACK (Scala/Spark, H-Hadoop, A-All things Apache, C-Cassandra, K-Kafka)

Our fictitious scenario: 
Let's assume that we inherited a small company "ISellInstoreAndOnline.com". The company is subdivided into logical groups: Stores, WebStore, Merchandising System, Distribution Systems and Inventory Management System.
These logical groups uses Apache Kafka as common message bus. The webstore uses REDIS session manager. Merchandising system is responsible for Catalog and Promo optimization.

Our goal is to improve promo efficiency by utilizing click stream data that the session provides us through Kafka. 
Later we will introduce <b>Hadoop & Lambda Architecture</b> to build promotions based on historical trends. 
  
 Usage: <br />
  Change application.conf under resources <br />
  Run "ClickStreamGenerator" under generators package to generate sample data <br />
  Run "BatchRDD" under batch package to run spark job <br />

Here is a simple Activity class:

      case class Activity(
                         timestamp: Long,
                         productId: Int,
                         userId: Int,
                         referrer: String,
                         retailPrice: Int,
                         productDiscountPct: Int,
                         cartDiscountPct: Int,
                         actionCode: Int,
                         marginPct: Int
                         )

Give we don't have a real world session manager, I built the package generators to provide random yet thoughtfully simulated data stream. 
product & cart level discount percentages represent promotions impacting retail price to produce effective retail, where as margin dictates our profitability.

Action Code indicates activity type: "0 -> Visit to Product Details Page", "1 -> Product Added to Cart", "2 -> Purchase". 

The session manager is responsible for capturing, maintaining click stream state and provide the data upon a <b> successful purchase </b> or after <b>user session expires (say 15 minutes)

Here is some sample data stream represented as JSON strings:

        {"timestamp":1483913095714,"productId":6219,"userId":18391,"referrer":"facebook","retailPrice":3,"productDiscountPct":10,"cartDiscountPct":0,"actionCode":1,"marginPct":15}
        {"timestamp":1483913095811,"productId":2965,"userId":57630,"referrer":"site","retailPrice":28,"productDiscountPct":0,"cartDiscountPct":0,"actionCode":2,"marginPct":25}
        {"timestamp":1483913095811,"productId":4182,"userId":97544,"referrer":"google","retailPrice":42,"productDiscountPct":10,"cartDiscountPct":0,"actionCode":0,"marginPct":30}
        {"timestamp":1483913095812,"productId":2565,"userId":78936,"referrer":"site","retailPrice":12,"productDiscountPct":0,"cartDiscountPct":0,"actionCode":2,"marginPct":25}
        {"timestamp":1483913095812,"productId":8305,"userId":39827,"referrer":"facebook","retailPrice":20,"productDiscountPct":20,"cartDiscountPct":10,"actionCode":2,"marginPct":25}
        {"timestamp":1483913095813,"productId":3748,"userId":13330,"referrer":"site","retailPrice":73,"productDiscountPct":0,"cartDiscountPct":0,"actionCode":2,"marginPct":30}
        {"timestamp":1483913095814,"productId":1781,"userId":94564,"referrer":"yahoo","retailPrice":93,"productDiscountPct":0,"cartDiscountPct":0,"actionCode":0,"marginPct":10}

<h3>Implementation Plan</h3>

Our spark code is responsible for generating in distributed in-memory data sets where we could perform map-reduce operations. 
<p> 1. We will start with generating RDD[Activity] and perform some analytics on it </p>
<p> 2. Generate Data Frames and register temp table so we could perform SQL operations on it </p>
<p> 3. Integrate with Kafka using spark-streams library </p>
<p> 4. Persist to Cassandra using spark-cassandra connector </p>
<p> 5. Introduce Hadoop & Lambda Architecture </p>

<h2>Investigate Spark Code and Improve it </h2>

      val conf = new SparkConf().setAppName(appName).setMaster(s"${sparkMaster}[${numCores}]")
      val sc = new SparkContext(conf)
      val sourceFile = tmpFile
      val input = sc.textFile(sourceFile)
      val inputRDD = input.flatMap { line => Some(parse(line).extract[Activity]) }
      val keyedByProduct = inputRDD.keyBy( a => a.productId).cache()
      println(keyedByProduct.count())
      val visitorsByProduct = keyedByProduct.mapValues( a => (a.userId)).distinct().countByKey()
      val sample = visitorsByProduct.take(100)
      println(sample)

All important SparkContext provides us necessary bells & whistles to perform our spark operations.
The following line is responsible for generating RDD[Activity] from RDD[String] that the context provided us.

      val inputRDD = input.flatMap { line => Some(parse(line).extract[Activity]) }

Just one line of code with lightening fast performance, but this is not fault tolerant, it could fail while parsing json string or while generating Activity object, let's improve.

      val inputRDD = input.flatMap { line =>
        Try(parse(line)).toOption.flatMap( jv => Try(jv.extract[Activity]).toOption.flatMap( v => Some(v)) )
      }

Now it is error resistant, this logic will ignore any fault lines. 
We don't like nested flat-maps, the code tend to look ugly after few levels of nesting. let's use scala for-comprehensions to make it developer friendly.
   
       val inputRDD = for {
          line <- input
          jv <- Try(parse(line)).toOption
          act <- Try(jv.extract[Activity]).toOption
       } yield act
       
This is great but I don't want to ignore errors, I want to handle them. Let's see how we could do that:

    def errorHandler[T](tv: Try[T]) : Option[T] = {
    tv match {
      case Success(v) => Some(v)
      case Failure(ex) => {
        println(s"${ex.getMessage} ${System.lineSeparator()} ${ex.getStackTrace}")
        None
      }
    }
    }  
    val inputRDD = for {
      line <- input
      jv <- errorHandler(Try(parse(line)))
      act <- errorHandler(Try(jv.extract[Activity]))
    } yield act

  This looks great.... This looks like a generic error handler, accepts Try of Something and handles it (log it, email it or whatever) then generates an Option of Something.
  Let's push it to utils so we could use it everywhere... Let's do the same for JSON parsing as well.... Here is the final logic
     
     
      val inputRDD = for {
        line <- input
        activity <- tryParse[Activity](line)
      } yield activity

 
We started with two lines of code and ended with two lines of code that is production ready, this is the power of Scala and functional composition.



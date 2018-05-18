package com.jxuao.spark


import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.parquet.format.event.TypedConsumer.SetConsumer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.from_json
import spray.json._
import java.util.Properties
import java.sql.Timestamp

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object DirectKafkaWordCount {

  def convertSeqJsValue(jsValue: Seq[JsValue]): Any = {
    jsValue match {
      case Seq(JsString(jsValue)) => jsValue.toString
      case Seq(JsNumber(jsValue)) => jsValue.toLong
      case Seq(JsString("true")) => true
      case Seq(JsString("false")) => false
      case _ => DeserializationException("Only support string|double|boolean")
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))


    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))


    val host = "192.168.56.100:5432"
    val db = "db_test_kafka_streaming"
    val url = s"jdbc:postgresql://$host/$db"
    val table = "t_word_count"

    val props = new Properties
    props.setProperty("driver", "org.postgresql.Driver")
    props.setProperty("user", "postgres")
    props.setProperty("password", "000000")


    val schema = StructType(
      StructField("word", DoubleType) ::
      StructField("count", DoubleType) :: Nil
//      StructField("ts", TimestampType) :: Nil
    )

    val jsonMessages = messages
        .map( record=> record.value())
        .map( row => JsonParser(row).asJsObject())
        .map( json => {
          val rCode = convertSeqJsValue(json.getFields("rcode")).toString
          val timeCost = convertSeqJsValue(json.getFields("timeCost")).asInstanceOf[Long]
          (rCode, (1L, timeCost))
        })

    val flatten = jsonMessages
        .reduceByKeyAndWindow( (x,y) => (x._1 + y._1, x._2 + y._2), windowDuration = Seconds(5), slideDuration = Seconds(1))
        .map( f => Row.fromSeq( Seq(System.currentTimeMillis(), f._2._2 / f._2._1.toDouble)))

    flatten.foreachRDD( rdd => {
//      val sql = new SparkSession(rdd.sparkContext)
        val sql = new SQLContext(rdd.sparkContext)

      sql.createDataFrame(rdd, schema)
        .write
        .mode(SaveMode.Append)
        .jdbc(url, table, props)
    })

    flatten.print()


    /*
    messages.foreachRDD( rdd => {
      val pairs = rdd
        .map(row => (row.timestamp(), JsonParser(row.value()).asJsObject()))
        .map(row => (convertSeqJsValue(row._2.getFields("rcode")).toString,
          (1L, convertSeqJsValue(row._2.getFields("timeCost")).asInstanceOf[Long], row._1)))

      val flatten = pairs
        .reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2, (x._3 + y._3)/2))
        .map(f => Row.fromSeq( Seq(f._1, f._2._2 / f._2._1.toDouble, new Timestamp(f._2._3))))


      val sql = new SparkSession(flatten.sparkContext)
//      val sql = new SQLContext(flatten.sparkContext)

      sql.createDataFrame(flatten, schema)
        .write
        .mode(SaveMode.Append)
        .jdbc(url, table, props)
    })
    */


    /*
    val pairs = messages.map(_.value()).map( x => JsonParser(x).asJsObject()).map( json => {
      val retCode = convertSeqJsValue((json.getFields("rcode"))).toString
      val timeCost = convertSeqJsValue(json.getFields("timeCost")).asInstanceOf[Long]
      (retCode, timeCost)
    })


    val windowPairs = pairs.reduceByKeyAndWindow( (x,y) => x+y, windowDuration = Seconds(5), slideDuration = Seconds(1))
//    val windowPairs = pairs.reduceByKey( (x,y) => x+y)

    windowPairs.print()


    windowPairs.foreachRDD( (rdd, time) => {

      rdd.foreach( row => {

        val dfSeq = Seq(
          (row._1, row._2)
        )
        val df = dfSeq.toDF("word", "count")

        df.write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", "jdbc:postgresql:192.168.56.100:5432/db_test_kafka_streaming")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "t_word_count")
          .option("user", "postgres")
          .option("password", "000000")
          .save()
      })
    })

*/

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println

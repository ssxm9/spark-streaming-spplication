package com.sparkstreaming.kafka.applauncher

import java.time.Duration
import java.util
import java.util.Properties

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.log4j.Logger
import Schemas.Schemas
import Schemas.TweetConsumerMessage
import java.time.Duration
import java.util
import java.util.Properties

import Schemas.Tweet
import org.apache.log4j.Logger

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.json4s.jackson.Serialization.write
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.codecs._
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.{Completed, MongoClient, MongoCollection, MongoDatabase, Observable, Observer}
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.conversions.Bson
import com.typesafe.config.{Config, ConfigFactory}


object AppLaunch {
  implicit val jsonDefaultFormats = DefaultFormats
  val logger: Logger = Logger.getLogger(this.getClass)
  var mongoClient: MongoClient = _
  var mongodb: MongoDatabase = _
  var tweet: MongoCollection[Document] = _
  lazy val tweetCollection: String = conf.getString("db.mongo-collections")
  val tweetConsumerMessageCodec: CodecProvider = Macros.createCodecProviderIgnoreNone(classOf[TweetConsumerMessage])
  implicit val codecRegistry: CodecRegistry = fromRegistries(fromProviders(tweetConsumerMessageCodec), DEFAULT_CODEC_REGISTRY)
  val conf: Config = ConfigFactory.load()
  val kafkaprop = "spark-streaming-kafka"
  def main(args: Array[String]): Unit = {
    consumeFromKafka(conf.getString(kafkaprop + ".topic"))
  }

  def consumeFromKafka(topic: String): Unit = {
    println(topic)
    val props = new Properties()
    props.put("bootstrap.servers", conf.getString(kafkaprop + ".brokers"))
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", conf.getString(kafkaprop + ".offsetReset"))
    props.put("group.id", conf.getString(kafkaprop + ".groupId"))
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val tweetRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(conf.getInt(kafkaprop + ".pollTimeoutInMin")))
      // for(record <- tweetRecords.asScala)
      // println(record.value())
      //
      tweetRecords.asScala.foreach(record => {
        logger.info(record.value())

        insertToMongo(TweetConsumerMessage(record.value()))
      })

      def getMongoConnection(mongoUrl: String): Unit = {
        mongoClient = MongoClient(mongoUrl)
      }

      def insertToMongo(record: TweetConsumerMessage): Unit = {
        consumer.commitSync()
        val uri = conf.getString("db.mongo-url")
//        val servers = "localhost:27017"
//        val dbName = "driver_pool"
//        val userName = "admin1"
//        val password = "password1"
//        val uri = s"mongodb://${userName}:${password}@${servers}/${dbName}?retryWrites=true&w=majority"
        println(uri)
        getMongoConnection(uri)
        mongodb = mongoClient.getDatabase((conf.getString("db.mongo-database")))
        tweet = mongodb.getCollection(tweetCollection)
        //        val s: String = write(record)
        val s1 = Json(DefaultFormats).write(record)
        val doc: Document = BsonDocument(s1)
        //        val doc: Bson = Document("$set" -> s1)
        val insertObservable = tweet.insertOne(doc)
        insertObservable.subscribe(new Observer[Completed] {
          override def onNext(result: Completed): Unit = println(s"onNext: $result")
          override def onError(e: Throwable): Unit = println(s"onError: $e")
          override def onComplete(): Unit = println("onComplete")
        })
      }
    }
  }
}

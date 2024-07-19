package org.esgi.project.streaming

import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import io.github.azhur.kafka.serde.PlayJsonSupport.toSerde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{InfoStatMovie,
                                          Likes,
                                          LikesWithTitle,
                                          MeanScoreForFilm,
                                          Views}

import java.io.InputStream
import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val groupnumber: String = "groupe-1"

  val applicationBaseName = s"web-events-stream-app-$groupnumber"
  val likesTopicName: String = "likes"
  val viewsTopicName: String = "views"

  val lastMinuteStoreName = "NumberViewsOfLast1Minute"
  val lastFiveMinutesStoreName = "NumberViewsOfLast5Minute"
  val MeanScorePerFilmStoreName = "MeanScorePerFilm"
  val TotalViewsPerFilmStoreName = "TotalViewsPerFilm"

  val builder: StreamsBuilder = new StreamsBuilder

  val likes: KStream[String, Likes] = builder.stream[String, Likes](likesTopicName)
  val views: KStream[String, Views] = builder.stream[String, Views](viewsTopicName)

  val viewsGroupedByMovie: KGroupedStream[Long, Views] = views.groupBy((key, value) => value.id)

  val windows1min: TimeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1))
  val viewsOfLast1Minute: KTable[Windowed[Long], InfoStatMovie] = viewsGroupedByMovie
    .windowedBy(windows1min)
    .aggregate(InfoStatMovie.empty)((k, v, agg) => agg.incrementation(v.view_category).attributeTitle(v.title))(
      Materialized.as(lastMinuteStoreName)
    )

  val windows5min: TimeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1))
  val viewsOfLast5Minutes: KTable[Windowed[Long], InfoStatMovie] = viewsGroupedByMovie
    .windowedBy(windows5min)
    .aggregate(InfoStatMovie.empty)((k, v, agg) => agg.incrementation(v.view_category).attributeTitle(v.title))(
      Materialized.as(lastFiveMinutesStoreName)
    )

  val viewsTotal: KTable[String, Long] = views
    .groupBy((k, v) => v.title)
    .count()(Materialized.as(TotalViewsPerFilmStoreName))

  val likesWithViews: KStream[String, LikesWithTitle] = likes.join(views)(
    (likes: Likes, views: Views) => LikesWithTitle(likes.id, views.title, likes.score),
    JoinWindows.of(Duration.ofMinutes(2))
  )

  val meanScorePerFilm: KTable[Long, MeanScoreForFilm] = likesWithViews
    .groupBy((_, value) => value.id)
    .aggregate(MeanScoreForFilm.empty)((_, v, agg) =>
      {
        agg.increment(v.score)
      }.computeMeanScore.attributeTitle(v.title)
    )(Materialized.as(MeanScorePerFilmStoreName))

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), buildProperties())
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run {
        streams.close
      }
    }))
    streams
  }

  def buildProperties(appName: Option[String] = None): Properties = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    import org.apache.kafka.streams.StreamsConfig
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("kafka.properties")

    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appName.getOrElse(applicationBaseName))
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\Sofianne\\Desktop\\dev\\kafka-stream")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.load(inputStream)
    properties
  }
}

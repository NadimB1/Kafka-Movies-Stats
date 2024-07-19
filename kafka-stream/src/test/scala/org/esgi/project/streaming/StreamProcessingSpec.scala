package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.{TestInputTopic, TopologyTestDriver}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.esgi.project.streaming.models.{Likes, Views, MeanScoreForFilm}
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {

  test("Views and Likes Processing") {
    val uniqueAppName = s"web-events-stream-app-${UUID.randomUUID().toString}"

    val testDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties(Some(uniqueAppName))
    )

    val viewsTopic: TestInputTopic[String, Views] = testDriver.createInputTopic(
      StreamProcessing.viewsTopicName,
      Serdes.stringSerde.serializer(),
      toSerializer
    )

    val likesTopic: TestInputTopic[String, Likes] = testDriver.createInputTopic(
      StreamProcessing.likesTopicName,
      Serdes.stringSerde.serializer(),
      toSerializer
    )

    viewsTopic.pipeInput("1", new Views(1, "Movie A", "start_only"))
    likesTopic.pipeInput("1", new Likes(1, 5.0))

    val viewsCountStore: KeyValueStore[String, Long] = testDriver.getKeyValueStore[String, Long](StreamProcessing.TotalViewsPerFilmStoreName)
    val meanScoreStore: KeyValueStore[Long, MeanScoreForFilm] = testDriver.getKeyValueStore[Long, MeanScoreForFilm](StreamProcessing.MeanScorePerFilmStoreName)

    assert(viewsCountStore.get("Movie A") === 1)

    val meanScoreForFilm = meanScoreStore.get(1L)
    assert(meanScoreForFilm.sum === 5.0)
    assert(meanScoreForFilm.count === 1.0)
    assert(meanScoreForFilm.meanScore === 5.0)
    assert(meanScoreForFilm.title === "Movie A")

    testDriver.close()
  }
}

package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MeanScoreForFilm(
                             sum: Double,
                             count: Double,
                             meanScore: Double,
                             title: String
                           ) {
  def increment(score: Double) = this.copy(sum = this.sum + score, count = this.count + 1)

  def computeMeanScore = this.copy(
    meanScore = this.sum / this.count
  )

  def attributeTitle(title: String) = this.copy(
    title = title
  )
}

object MeanScoreForFilm {
  implicit val format: OFormat[MeanScoreForFilm] = Json.format[MeanScoreForFilm]

  def empty: MeanScoreForFilm = MeanScoreForFilm(0, 0, 0, "")
}


package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class MeanScorePerFilmResponse(
                                     title: String,
                                     meanScore: Double
                                   )


object MeanScorePerFilmResponse {
  implicit val format: OFormat[MeanScorePerFilmResponse] = Json.format[MeanScorePerFilmResponse]
}

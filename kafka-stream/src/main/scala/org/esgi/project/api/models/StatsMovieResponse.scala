package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class StatsMovieResponse(
                               past: MovieStatResponse,
                               last_minute: MovieStatResponse,
                               last_five_minutes: MovieStatResponse
                             )


object StatsMovieResponse {
  implicit val format: OFormat[StatsMovieResponse] = Json.format[StatsMovieResponse]

}
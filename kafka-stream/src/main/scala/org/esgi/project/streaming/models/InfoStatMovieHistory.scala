package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class InfoStatMovieHistory(timestamp: Long, data: InfoStatMovie)

object InfoStatMovieHistory {
  implicit val format: OFormat[InfoStatMovieHistory] = Json.format[InfoStatMovieHistory]
}

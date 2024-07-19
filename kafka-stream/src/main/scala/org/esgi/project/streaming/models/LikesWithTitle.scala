package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class LikesWithTitle(
                           id: Long,
                           title: String,
                           score: Double
                         )
object LikesWithTitle {
  implicit val format: OFormat[LikesWithTitle] = Json.format[LikesWithTitle]
}
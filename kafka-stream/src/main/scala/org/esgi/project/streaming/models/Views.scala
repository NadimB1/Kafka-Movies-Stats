package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Views(
                  id: Long,
                  title: String,
                  view_category: String
                )



object Views {
  implicit val format: OFormat[Views] = Json.format[Views]
}

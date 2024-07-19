package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class TitleViewsResponse(
                               title: String,
                               views: Long
                             )

object TitleViewsResponse {
  implicit val format: OFormat[TitleViewsResponse] = Json.format[TitleViewsResponse]
}
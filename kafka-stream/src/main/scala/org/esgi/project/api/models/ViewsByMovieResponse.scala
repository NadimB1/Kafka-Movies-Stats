package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class ViewsByMovieResponse(
                                 id: Long,
                                 title: String,
                                 view_count : Long,
                                 stats: StatsMovieResponse
                               ) {

}
object ViewsByMovieResponse {
  implicit val format: OFormat[ViewsByMovieResponse] = Json.format[ViewsByMovieResponse]

}







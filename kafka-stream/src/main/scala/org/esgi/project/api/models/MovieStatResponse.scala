package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class MovieStatResponse(
                              start_only: Long,
                              half: Long,
                              full : Long
                            ){
  def cumul(start_only: Long, half: Long, full: Long): MovieStatResponse ={
    this.copy(
      start_only=this.start_only + start_only,
      half=this.half + half,
      full=this.full + full
    )
  }

}


object MovieStatResponse {
  implicit val format: OFormat[MovieStatResponse] = Json.format[MovieStatResponse]

  def empty: MovieStatResponse = MovieStatResponse(start_only = 0, half = 0, full = 0)
}

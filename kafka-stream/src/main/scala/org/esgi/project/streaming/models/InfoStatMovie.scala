package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class InfoStatMovie(
                          title: String,
                          half: Long,
                          full : Long,
                          start_only: Long,
                          total: Long
                        )
{
  def incrementation(view_category: String): InfoStatMovie={
    view_category match {
      case "half" => this.copy(half = this.half + 1, total = this.total + 1)
      case "full" => this.copy(full = this.full + 1, total = this.total + 1)
      case "start_only" => this.copy(start_only = this.start_only + 1, total = this.total + 1)
      case _ => this.copy()
    }
  }
  def attributeTitle(title:String) ={
    this.copy(title = title)
  }
}

object InfoStatMovie {
  implicit val format: OFormat[InfoStatMovie] = Json.format[InfoStatMovie]

  def empty: InfoStatMovie  = InfoStatMovie(half = 0,full = 0,start_only = 0, title = "", total = 0)
}


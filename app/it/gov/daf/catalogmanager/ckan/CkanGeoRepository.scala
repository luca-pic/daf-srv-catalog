package it.gov.daf.catalogmanager.ckan

import java.time.{Instant, LocalDateTime, ZoneOffset}

import it.gov.daf.model.{Dataset, Relationship}
import it.gov.daf.config.CkanGeoConfig
import javax.inject.Inject
import play.api.libs.circe.Circe
import io.circe.generic.auto._
import io.circe.syntax._


class CkanGeoRepository @Inject()(implicit ckanGeoConfig: CkanGeoConfig) extends Circe {

  def createCkanGeoCatalog(metacatalog: Dataset) = {
    def formatTimestap(timestamp: String) = {
      val arrayTimestamp = timestamp.split('.')
      val sec = arrayTimestamp(1)
      val secOut = sec.length match {
        case x if x > 6 => sec.substring(0, 6)
        case x if x < 6 => sec + "0".*(6-x)
        case _ => sec
      }
      List(arrayTimestamp(0), secOut).mkString(".")
    }
    val timestamp = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).toString
    metacatalog.copy(
      relationships_as_object = Some(List(Relationship(None, None, Some("has_derivation"), Some("opendataDaf")))),
      resources = Some(metacatalog.resources.get.map(f => f.copy(created = Some(formatTimestap(timestamp)))))
    ).asJson.toString().replace("privatex", "private")

  }

}

package it.gov.daf.catalogmanager.voc

import java.io.FileInputStream

import play.api.libs.json._
import it.gov.daf.model.{Error, KeyValue, VocKeyValueSubtheme}

import scala.concurrent.Future

class VocRepository {

  def listThemeAll(): Future[Either[Error, Seq[KeyValue]]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json: Option[JsArray] = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}

    json match {
      case Some(s) => Future.successful(Right(s.value.map(x=>KeyValue((x \ "theme_daf_code").as[String], (x \ "theme_daf_ita").as[String]))))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }

  def listSubthemeAll(): Future[Either[Error, Seq[VocKeyValueSubtheme]]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}

    json match {
      case Some(s) =>
        val seqSubtheme = s.value.map(x => ((x \ "theme_daf_code").as[String],
        (x \ "subthemes").as[List[JsValue]])).flatMap { x =>

        x._2.map { y =>
          VocKeyValueSubtheme((y \ "subtheme_daf_code").as[String], (y \ "subtheme_daf_ita").as[String], x._1, None)
        }
      }
        Future.successful(Right(seqSubtheme))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }


  def listSubtheme(themeId: String): Future[Either[Error, Seq[KeyValue]]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}
    json match {
      case Some(s) =>
        val seqSubtheme = s.value.map(x => ((x \ "theme_daf_code").as[String],
        (x \ "subthemes").as[List[JsValue]]))
        .filter(x => x._1.equals(themeId)).flatMap { x =>

        x._2.map { y =>
          KeyValue((y \ "subtheme_daf_code").as[String], (y \ "subtheme_daf_ita").as[String])
        }
      }
        Future.successful(Right(seqSubtheme))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }
  def daf2dcatTheme(dafThemeId: String): Future[Either[Error, Seq[KeyValue]]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}
    json match {
      case Some(s) =>
        val seq = s.value.map(x => ((x \ "theme_daf_code").as[String],
        (x \ "theme_ita").as[String],
        (x \ "theme_code").as[String]))
        .filter(x => x._1.equals(dafThemeId))
        .map(x=> KeyValue(x._2, x._3))
        Future.successful(Right(seq))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }
  def daf2dcatSubtheme(dafThemeId: String, dafSubthemeId: String = "__-1NOFILTER__"): Future[Either[Error, Seq[VocKeyValueSubtheme]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}


    json match {
      case Some(s) =>
        val seq = s.value.map(x => (
        (x \ "theme_daf_code").as[String],
        (x \ "theme_code").as[String],
        (x \ "theme_ita").as[String],
        (x \ "subthemes").as[List[JsValue]]))
        .filter(x => x._1.equals(dafThemeId)).flatMap { x =>
        x._4.map { y =>
          //(Theme Ita, SubthemesIta, Subtheme Daf Code)
          (x._2, x._3, (y \ "subthemes_ita").as[List[List[String]]], (y \ "subtheme_daf_code").as[String])
        }
      }
        .filter(x => x._4.equals(dafSubthemeId) || dafSubthemeId.equals("__-1NOFILTER__")).flatMap { x =>
        x._3.map { y =>
          VocKeyValueSubtheme(y(0), y(1), x._1, Some(x._2))
        }
      }
        Future.successful(Right(seq))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }

  def listDcatThemeAll(): Future[Either[Error, Seq[KeyValue]]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}

    json match {
      case Some(s) => Future.successful(Right(s.value.map(x=> KeyValue((x \ "theme_code").as[String], (x \ "theme_ita").as[String]))))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }

  def listDcatSubthemeAll(): Future[Either[Error, Seq[VocKeyValueSubtheme]]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}

    json match {
      case Some(s) =>
        val seq = s.value.map(x => ((x \ "theme_code").as[String],
        (x \ "subthemes").as[List[JsValue]])).flatMap { x =>

        x._2.map { y => //subthemes

          (x._1, (y \ "subthemes_ita").as[List[List[String]]])
        }
      }.flatMap { x =>
        x._2.map { y =>
          VocKeyValueSubtheme(y(0), y(1), x._1, None)
        }
      }
        Future.successful(Right(seq))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }
  def listDcatSubtheme(dcatapitThemeId: String): Future[Either[Error, Seq[KeyValue]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}

    json match {
      case Some(s) =>
        val seq = s.value.map(x => ((x \ "theme_code").as[String],
        (x \ "subthemes").as[List[JsValue]])).flatMap { x =>

        x._2.map { y => //subthemes

          (x._1, (y \ "subthemes_ita").as[List[List[String]]])
        }
      }
        .filter(x => x._1.equals(dcatapitThemeId)).flatMap { x =>
        x._2.map { y =>
          KeyValue(y(0), y(1))
        }
      }
        Future.successful(Right(seq))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }

  def dcat2DafTheme(dcatapitThemeId: String): Future[Either[Error, Seq[KeyValue]]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try { (Json.parse(stream) \ "voc").asOpt[JsArray]} finally {stream.close()}

    json match {
      case Some(s) =>
        val seqKeyValue = s.value.map{x=>
        ((x \ "theme_daf_code").as[String], (x \ "theme_daf_ita").as[String], (x \ "theme_code").as[String])
      }
        .filter(x=> x._3.equals(dcatapitThemeId))
        .map(x=>KeyValue(x._1, x._2))
        Future.successful(Right(seqKeyValue))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Error", None)))
    }
  }

  def dcat2DafSubtheme(dcatapitThemeId: String, dcatapitSubthemeId: String): Future[Either[Error, Seq[VocKeyValueSubtheme]]] = {
    val stream = new FileInputStream("data/voc/cv_theme-subtheme_bk.json")
    val json = try {
      (Json.parse(stream) \ "voc").asOpt[JsArray]
    } finally {
      stream.close()
    }

    json match {
      case Some(s) =>
        val seq = s.value.map(x => (
        (x \ "theme_daf_code").as[String],
        (x \ "subthemes").as[List[JsValue]],
        (x \ "theme_code").as[String]))
        .filter(x => x._3.equals(dcatapitThemeId)).flatMap { x =>

        x._2.map { y =>
          (VocKeyValueSubtheme((y \ "subtheme_daf_code").as[String], (y \ "subtheme_daf_ita").as[String], x._1, None), (y \ "subthemes_ita").as[List[List[String]]])
        }
      }
        .filter(x => x._2.map(_ (0)).contains(dcatapitSubthemeId))
        .map(x => x._1)
        Future.successful(Right(seq))
      case None =>
        println("VocRepositoryFile - Error occurred with Json")
        Future.successful(Left(Error(Some(500), "Internal Server Errror", None)))
    }
  }

}

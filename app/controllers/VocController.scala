package controllers

import io.swagger.annotations.{Api, ApiOperation}
import it.gov.daf.catalogmanager.voc.VocRepository
import it.gov.daf.common.utils.RequestContext.execInContext
import it.gov.daf.model.{DatasetNameFields, KeyValue, MetaCatalog, VocKeyValueSubtheme}
import javax.inject.Inject
import play.api.libs.circe.Circe
import io.circe.generic.auto._
import play.api.mvc._

import scala.concurrent.Future


@Api
class VocController @Inject()  extends Controller with Circe {


  private val vocRepository = new VocRepository

  @ApiOperation(value = "get the list of all DAF themes", response = classOf[Seq[KeyValue]])
  def vocThemesGetAll() = Action.async { implicit request =>
    execInContext[Future[Result]]("vocThemesGetAll") { () =>
      handleException[Seq[KeyValue]] {
        vocRepository.listThemeAll()
      }
    }
  }

  @ApiOperation(value = "get the list of all DAF subthemes", response = classOf[Seq[VocKeyValueSubtheme]])
  def vocSubthemesGetAll() = Action.async { implicit request =>
    execInContext[Future[Result]]("vocSubthemesGetAll") { () =>
      handleException[Seq[VocKeyValueSubtheme]] {
        vocRepository.listSubthemeAll()
      }
    }
  }

  @ApiOperation(value = "get the list of all DAF subtheme by id", response = classOf[Seq[KeyValue]])
  def vocSubthemesGetById(themeId: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("vocSubthemesGetAll") { () =>
      handleException[Seq[KeyValue]] {
        vocRepository.listSubtheme(themeId)
      }
    }
  }

  @ApiOperation(value = "get a DAF Subtheme associated to a given DCATAPIT theme", response = classOf[Seq[KeyValue]])
  def vocDcat2DafTheme(themeId: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("vocDcat2DafTheme") { () =>
      handleException[Seq[KeyValue]] {
        vocRepository.dcat2DafTheme(themeId)
      }
    }
  }

  @ApiOperation(value = "get a DAF Theme associated to a given DCATAPIT theme", response = classOf[Seq[VocKeyValueSubtheme]])
  def vocDcat2DafSubtheme(themeId: String, subthemeId: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("vocDcat2DafSubtheme") { () =>
      handleException[Seq[VocKeyValueSubtheme]] {
        vocRepository.daf2dcatSubtheme(themeId, subthemeId)
      }
    }
  }

  @ApiOperation(value = "get a DAF Themes", response = classOf[Seq[KeyValue]])
  def vocDcatThemeGetAll(themeId: String, subthemeId: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("vocDcatThemeGetAll") { () =>
      handleException[Seq[KeyValue]] {
        vocRepository.listDcatThemeAll()
      }
    }
  }

  @ApiOperation(value = "get the list of all DCATAPIT subthemes", response = classOf[Seq[VocKeyValueSubtheme]])
  def vocDcatSubthemesGetAll = Action.async { implicit request =>
    execInContext[Future[Result]]("vocDcatSubthemesGetAll") { () =>
      handleException[Seq[VocKeyValueSubtheme]] {
        vocRepository.listDcatSubthemeAll()
      }
    }
  }

  @ApiOperation(value = "get the list of all DCATAPIT subthemes by id", response = classOf[Seq[KeyValue]])
  def vocDcatSubthemesGetById(dcatapitThemeId: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("vocDcatSubthemesGetById") { () =>
      handleException[Seq[KeyValue]] {
        vocRepository.listDcatSubtheme(dcatapitThemeId)
      }
    }
  }

  @ApiOperation(value = "get a DCATAPIT Theme associated to a given DAF theme", response = classOf[Seq[KeyValue]])
  def vocDaf2DcatTheme(themeId: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("vocDaf2DcatTheme") { () =>
      handleException[Seq[KeyValue]] {
        vocRepository.daf2dcatTheme(themeId)
      }
    }
  }

  @ApiOperation(value = "get a DCATAPIT Subtheme associated to a given DAF theme", response = classOf[Seq[VocKeyValueSubtheme]])
  def vocDaf2DcatSubtheme(themeId: String, subthemeId: String) = Action.async { implicit request =>
    execInContext[Future[Result]]("vocDaf2DcatSubtheme") { () =>
      handleException[Seq[VocKeyValueSubtheme]] {
        vocRepository.dcat2DafSubtheme(themeId, subthemeId)
      }
    }
  }

}

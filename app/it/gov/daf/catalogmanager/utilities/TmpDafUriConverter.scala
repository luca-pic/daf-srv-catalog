package it.gov.daf.catalogmanager.utilities


sealed trait DatasetType
case object Standard extends DatasetType
case object Ordinary extends DatasetType
case object OpenData extends DatasetType
case object Derived extends DatasetType

class TmpDafUriConverter(
                       dsType: DatasetType,
                       organization: String,
                       theme: String,
                       subTheme: String,
                       dsName: String
                     ) {

  private val logicalUri = dsType match {
    case Standard =>
      s"daf://dataset/standard/$theme" + "__" + s"$subTheme/$dsName"
    case Ordinary =>
      s"daf://dataset/$organization/$theme" + "__" + s"$subTheme/$dsName"
    case OpenData =>
      s"daf://opendata/$dsName"
    case Derived =>
      s"daf://derived/$theme" + "__" + s"$subTheme/$dsName"
  }

  private val physicalUri = dsType match {
    case Standard =>
      s"/daf/standard/$theme" + "__" + s"$subTheme/$dsName"
    case Ordinary =>
      s"/daf/ordinary/$organization/$theme" + "__" + s"$subTheme/" + organization + "_o_" + dsName
    case OpenData =>
      s"/daf/opendata/$dsName"
    case Derived =>
      s"daf://derived/$theme" + "__" + s"$subTheme/$dsName"
  }

  def toLogicalUri: String = logicalUri

  def toPhysicalUri(): String = physicalUri

}
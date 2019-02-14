package it.gov.daf.catalogmanager.utils

/**
  * Created by ale on 29/05/17.
  */

package object datastructures {

  object DatasetType extends Enumeration {
    val STANDARD = Value("standard")
    val ORDINARY = Value("ordinary")
    val DERIVED = Value("derived_sql")
    val RAW = Value("raw")

    def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)
  }

}

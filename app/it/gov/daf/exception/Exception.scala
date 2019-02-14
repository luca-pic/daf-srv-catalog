package it.gov.daf.exception

final case class NoOrgDatasetException(private val message: String = "Not found dataset organization",
                                       private val cause: Throwable = None.orNull
                                      ) extends Exception(message, cause)

final case class NameAlredyExistException(private val message: String = "Name alredy exist in db",
                                          private val cause: Throwable = None.orNull
                                         ) extends Exception(message, cause)

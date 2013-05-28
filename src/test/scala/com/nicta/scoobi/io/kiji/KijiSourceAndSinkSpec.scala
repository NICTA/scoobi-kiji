package com.nicta.scoobi
package io
package kiji

import KijiInput._
import Scoobi._
import testing.{KijiCommands, KijiSpecification}
import scalaz.syntax.monad._
import org.specs2.specification.Grouped
import org.kiji.schema.KijiDataRequest

class KijiSourceAndSinkSpec extends KijiSpecification with Grouped { def is = s2"""

 A Kiji table can be used as a Source for DLists. You first need to pass the table path, and then it is possible to read
 values by passing in a Kiji DataRequest object
  + for example to retrieve the most recent values for a single column
  + the most recent values for a single column, in a given time range

"""

  "source" - new group with KijiCommands {
    eg := { implicit sc: SC =>
      onTable("table", "simple.json") {
        put("1", "family", "column", "hello")  >>
        put("1", "family", "column", "hello2") >> get {
          val request: KijiDataRequest = KijiDataRequest.create("family")
          val values = fromRequest(sc.configuration.get(KIJI_TEST_URI), layoutDir+"simple.json", "table", request).run.map(_.family[String]("column"))
          values must_== Vector("hello2")
        }
      }
    }
  }

}


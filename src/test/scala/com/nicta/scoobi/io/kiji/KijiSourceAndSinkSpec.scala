/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        put("1", "family", "column", "hello2") >> tableRun { table =>
          val request: KijiDataRequest = KijiDataRequest.create("family")
          val values = fromRequest(table, request).map(_.family[String]("column")).run
          values must_== Vector("hello2")
        }
      }
    }
  }

}


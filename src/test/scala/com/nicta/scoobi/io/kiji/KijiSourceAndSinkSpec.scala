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
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef
import shapeless._
import java.io.{DataInput, DataOutput}

class KijiSourceAndSinkSpec extends KijiSpecification with Grouped { def is = s2"""

 A Kiji table can be used as a Source for DLists. You first need to pass the table path, and then it is possible to read
 values by passing in a Kiji DataRequest object

  for example to retrieve the most recent values
    + for a single column
    + for several columns

"""

  "use a Kiji table as a source" - new group with KijiCommands {
    eg := { implicit sc: SC =>
      onTable("table", "simple.json") {
        put("1", "family", "column", "hello")  >>
        put("1", "family", "column", "hello2") >> tableRun { table =>
          val request: KijiDataRequest = KijiDataRequest.create("family")
          val values = fromRequest(table, request).map(_.family_column[String]).run
          values must_== Vector("hello2")
        }
      }
    }

    eg := { implicit sc: SC =>
      onTable("table", "twoColumns.json") {
        put("1", "family", "column1", "hello")  >>
        put("1", "family", "column1", "hello2") >>
        put("1", "family", "column2", 1)  >>
        put("1", "family", "column2", 2) >>
        put("2", "family", "column1", "hi")  >>
        put("2", "family", "column1", "hi2") >>
        put("2", "family", "column2", 3)  >>
        put("2", "family", "column2", 4) >>
          tableRun { table =>
          val request: KijiDataRequest = KijiDataRequest.builder.
            addColumns(ColumnsDef.create.add("family", "column1").add("family", "column2")).build

          val values = fromRequest(table, request).map(_.family[String :: Int :: HNil]("column1", "column2")).run
          values must_== Vector("hello2" :: 2 :: HNil, "hi2" :: 4 :: HNil)
        }
      }
    }
  }

  implicit def hnilHasWireFormat: WireFormat[HNil] = new WireFormat[HNil] {
    def toWire(x: HNil, out: DataOutput) {}
    def fromWire(in: DataInput) = HNil
  }

  implicit def hlistHasWireFormat[T, H1 <: HList](implicit wft: WireFormat[T], wfh1: WireFormat[H1]): WireFormat[T :: H1] = new WireFormat[T :: H1] {
    def toWire(hlist: T :: H1, out: DataOutput) = hlist match {
      case head :: rest => wft.toWire(head, out); wfh1.toWire(rest, out)
    }
    def fromWire(in: DataInput) = wft.fromWire(in) :: wfh1.fromWire(in)
  }

}


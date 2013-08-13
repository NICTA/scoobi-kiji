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
import KijiOutput._
import Scoobi._
import testing.{KijiCommands, KijiSpecification}
import scalaz.syntax.monad._
import org.specs2.specification.Grouped
import org.kiji.schema._
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef
import org.kiji.schema.filter.ColumnValueEqualsRowFilter
import shapeless._
import com.nicta.scoobi.testing.TestFiles
import org.specs2.matcher._
import org.kiji.mapreduce.tools.KijiBulkLoad
import org.apache.avro.Schema

class KijiSourceAndSinkSpec extends KijiSpecification with Grouped with TestFiles with FileMatchers { def is = s2"""

 This project provides some specific source and sinks to work with Kiji tables. A `KijiTable` can be used as a `Source` for DLists and a `DList` can be persisted as a `HFile` which can then be bulk-loaded into Kiji.

Sources
=======

 In order to use a Kiji table as a Data Source for a DList, you need to pass the source table, a `KijiDataRequest`, and then you get a `DList` which values are `KijiDataRows`

  for example to retrieve the most recent values
    + for a single column
    + for several columns
    + using a row filter

Sinks
=====

 It is possible, as well, to persist a Scoobi collection to a `HFile` which can then be loaded into Kiji. The `DList` must contain elements of type `EntityValue`, encapsulating an `entityId`, a column (family + qualifier), a timestamp and a value.

  + persisting a simple 'hello' value should create a `HFile`

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

    eg := { implicit sc: SC =>
      onTable("table", "twoColumns.json") {
        put("1", "family", "column1", "true")  >>
        put("2", "family", "column1", "false") >>
        put("3", "family", "column1", "true")  >>
        put("4", "family", "column1", "false")  >>
        put("1", "family", "column2", 1) >>
        put("2", "family", "column2", 2) >>
        put("3", "family", "column2", 3) >>
        put("4", "family", "column2", 4) >>
          tableRun { table =>
          val request: KijiDataRequest = KijiDataRequest.builder.
            addColumns(ColumnsDef.create.add("family", "column1").add("family", "column2")).build

          val filterString = new DecodedCell[String](Schema.create(Schema.Type.STRING), "true")
          val filter = new ColumnValueEqualsRowFilter("family", "column1", filterString)
          val values = fromRequest(table, request, filter=Some(filter)).map(_.family[String :: Int :: HNil]("column1", "column2")).run
          values must_== Vector("true" :: 1 :: HNil, "true" :: 3 :: HNil)
        }
      }
    }
  }

  "use a HFile as a sink" - new group with KijiCommands {
    eg := { implicit sc: SC =>
      onTable("table", "simple.json") {
        tableRun { table =>
          val file = createTempDir("hbase")
          implicit val wf = EntityValue.wireFormat[String](table.getLayout)
          DList(EntityValue.create(table.getEntityId("1"), "family", "column", 123456, "hello")).toHFile(file.getPath, table).run
          file must beAFile
          file.listFiles must not be empty

          // now load the hfile back into Kiji
          val loader = new KijiBulkLoad
          loader.setConf(sc.configuration)
          loader.toolMain(java.util.Arrays.asList("--hfile="+file.getPath, "--table="+table.getURI.toString))

          val request: KijiDataRequest = KijiDataRequest.create("family")
          val values = fromRequest(table, request).map(_.family_column[String]).run
          values must_== Vector("hello")
        }
      }.pendingUntilFixed
    }
  }

}


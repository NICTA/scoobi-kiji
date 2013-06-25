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

import testing._
import testing.mutable.KijiSpecification
import org.kiji.schema._
import impl._
import org.apache.hadoop.hbase._
import io.ImmutableBytesWritable
import core.WireFormat
import java.io._
import org.specs2.specification.Grouped
import org.specs2.mock.Mockito
import scalaz._; import Scalaz._
import EntityRow._

class EntityRowSpec extends KijiSpecification with Grouped with Mockito {                            s2"""

 An EntityRow encapsulates the result of a KijiDataRequest for a given EntityId

 + an EntityId has a WireFormat which just serializes the key bytes
 + an EntityRow has a WireFormat for the key and the row values
                                                                                                        """
  "wireformat" - new group with KijiCommands {
    eg := { implicit sc: SC =>
      val  layout = createLayout("simple.json")
      implicit val wf = EntityIdWireFormat.entityIdWireFormat(layout)

      val entityId = EntityIdFactory.getFactory(layout).getEntityId("key")
      serialisationIsOkWith(entityId)
    }
    eg := { implicit sc: SC =>
      onTable("table", "simple.json") {
        tableRun { table =>
          val layout = createLayout("simple.json")
          val entityId = EntityIdFactory.getFactory(layout).getEntityId("key")
          val keyValues = Array(KeyValue.LOWESTKEY)

          val result = new org.apache.hadoop.hbase.client.Result(keyValues)

          val entityValue = EntityRow(entityId, new HBaseKijiRowData(table.asInstanceOf[HBaseKijiTable], KijiDataRequest.create("family"), entityId, result, null))
          implicit val wf = entityValueHasWireFormat(table, KijiDataRequest.create("family"))
          val serialised = serialise(entityValue)
          val deserialised = {
            val bais = new ByteArrayInputStream(serialised)
            wf.fromWire(new DataInputStream(bais), table)
          }
          deserialised must_== entityValue
        }
      }
    }

  }

  def serialisationIsOkWith[T: WireFormat](x: T): Boolean = deserialise[T](serialise(x)) must_== x

  def serialise[T: WireFormat](obj: T): Array[Byte] = {
    val bs = new ByteArrayOutputStream
    implicitly[WireFormat[T]].toWire(obj, new DataOutputStream(bs))
    bs.toByteArray
  }

  def deserialise[T: WireFormat](raw: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(raw)
    implicitly[WireFormat[T]].fromWire(new DataInputStream(bais))
  }

  override def context = inMemory.flatMap(sc => new KijiContext(sc))
}

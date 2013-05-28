package com.nicta.scoobi
package io
package kiji

import com.nicta.scoobi.io.kiji.testing.{KijiContext, KijiCommands}
import testing.mutable.KijiSpecification
import core.WireFormat
import java.io.{DataInputStream, ByteArrayInputStream, DataOutputStream, ByteArrayOutputStream}
import org.specs2.specification.Grouped
import scalaz._; import Scalaz._
import org.kiji.schema._
import impl._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.specs2.mock.Mockito
import EntityRow._
import com.nicta.scoobi.io.kiji.KijiInput._

class EntityValueSpec extends KijiSpecification with Grouped with Mockito {                                          s2"""

 An EntityValue encapsulates a value of type A, with an associated WireFormat and its EntityId

 + an EntityId has a WireFormat which just serializes the key bytes
 + an EntityValue has a WireFormat for the key and the value
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
          val result = new org.apache.hadoop.hbase.client.Result(new ImmutableBytesWritable("result".getBytes))
          val entityValue = EntityRow(entityId, new HBaseKijiRowData(entityId, KijiDataRequest.create("family"), table.asInstanceOf[HBaseKijiTable], result))
          implicit val wf = entityValueHasWireFormat(layout, table, KijiDataRequest.create("family"))

          serialisationIsOkWith(entityValue)
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

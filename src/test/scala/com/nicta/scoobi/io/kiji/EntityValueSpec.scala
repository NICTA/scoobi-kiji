package com.nicta.scoobi
package io
package kiji

import testing.KijiCommands
import testing.mutable.KijiSpecification
import org.kiji.schema.{EntityIdFactory, EntityId}
import core.WireFormat
import java.io.{DataInputStream, ByteArrayInputStream, DataOutputStream, ByteArrayOutputStream}

class EntityValueSpec extends KijiSpecification {                                                     s2"""

 An EntityValue encapsulate a value of type A, with an associated WireFormat and its EntityId
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
      val  layout = createLayout("simple.json")
      implicit val wf = EntityIdWireFormat.entityIdWireFormat(layout)

      val entityId = EntityIdFactory.getFactory(layout).getEntityId("key")
      val entityValue = EntityValue(entityId, "string")
      serialisationIsOkWith(entityValue)
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

  override def context = inMemory
}

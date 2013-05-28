package com.nicta.scoobi.io
package kiji
package testing

import org.specs2.specification.Fixture
import org.specs2.execute.AsResult
import com.nicta.scoobi.testing.ScoobiSpecification
import com.nicta.scoobi.core.ScoobiConfiguration
import scalaz.Scalaz._
import com.nicta.scoobi.impl.Configurations._
import org.apache.hadoop.hbase.HBaseConfiguration
import java.util.concurrent.atomic.AtomicLong
import org.kiji.schema.{Kiji, KijiInstaller, KijiURI}
import KijiContext._

abstract class KijiSpecification extends ScoobiSpecification {
  override def context = super.context.flatMap(sc => new KijiContext(sc))
}

case class KijiContext(sc: ScoobiConfiguration) extends Fixture[ScoobiConfiguration] {
  /** Counter for fake HBase instances. */
  private lazy val kijiConfiguration = HBaseConfiguration.create

  lazy val kiji = {
    val instanceName = String.format("%s_%s", "instance", "test")
    val hbaseAddress = f".fake.%s-%d".format(instanceName, instanceCounter.getAndIncrement)
    val uri = KijiURI.newBuilder(String.format("kiji://%s/%s", hbaseAddress, instanceName)).build
    sc.set(KIJI_TEST_URI, uri)
    KijiInstaller.get.install(uri, kijiConfiguration)
    Kiji.Factory.open(uri, kijiConfiguration)
  }

  def apply[R : AsResult](f: ScoobiConfiguration => R) =
    try     AsResult(f(configureForKiji(sc)))
    finally teardown


  private def configureForKiji(scoobiConf: ScoobiConfiguration): ScoobiConfiguration = {
    kiji
    scoobiConf.configuration.updateWith(kijiConfiguration) { case (k,v) => (k,v) }
    scoobiConf
  }

  private def teardown {
    kiji.release
    KijiInstaller.get.uninstall(kiji.getURI, kijiConfiguration)
  }
}

object KijiContext {
  lazy val instanceCounter = new AtomicLong
  lazy val KIJI_TEST_URI = "kiji.test.uri"
}
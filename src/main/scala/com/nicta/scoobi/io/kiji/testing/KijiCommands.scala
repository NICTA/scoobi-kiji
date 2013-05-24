package com.nicta.scoobi
package io
package kiji
package testing

import java.io.FileInputStream
import scalaz._
import scalaz.Scalaz._
import core.ScoobiConfiguration
import org.specs2.execute.{Result, AsResult}
import org.kiji.schema._
import org.kiji.schema.layout.KijiTableLayout

/**
 * This trait provides high-level methods to interact with a Kiji Table by making sure that resources (the Kiji instance, the table,
 * readers, writers) are properly opened and closed.
 *
 * Usage:
 *
 * onTable("table name", "table layout path") {
 *   put(...) >>
 *   put(...) >> get {
 *     ...
 *     ok
 *   }
 * }
 *
 * The last operation must return a specs2 result
 *
 */
trait KijiCommands {

  lazy val KIJI_TEST_URI = "kiji.test.uri"
  protected val layoutDir = "src/test/resources/layout/"

  /** create a layout from the layout directory */
  def createLayout(layout: String)  =
    KijiTableLayout.createFromEffectiveJson(new FileInputStream(layoutDir+layout))

  /** open a table with a specific name and layout and execute Kiji operations on this table */
  def onTable[A](tableName: String, layout: String)(onTable: RunKijiOperations[Result])(implicit sc: ScoobiConfiguration): Result =
    instance(table(tableName, layout)(onTable)).run(sc)

  /** @return a put operation */
  def put(id: AnyRef, family: String, qualifier: String, value: Any): RunKijiOperations[Unit] =
    RunKijiOperations(ops => ops.put(id, family, qualifier, value))

  /** @return kiji rows accessible by entity id */
  def rows(request: KijiDataRequest): RunKijiOperations[KijiRows] =
    RunKijiOperations(ops => ops.rows(request))

  /** execute a side-effecting operation */
  def run[A](r: =>Unit): RunKijiOperations[Unit] =
    RunKijiOperations(ops => ops.run(r))

  /** execute an operation using the whole table, like a Kiji gather operation */
  def tableRun[A](run: KijiTable => A): RunKijiOperations[A] =
    RunKijiOperations(ops => ops.tableRun(run))

  /** execute an operation and return a Result */
  def get[R : AsResult](result: =>R): RunKijiOperations[Result] =
    RunKijiOperations(ops => ops.get(AsResult(result)))

  type SC = ScoobiConfiguration

  /** type for objects depending on a Scoobi configuration */
  type ConfiguredT[F[+_], A] = ReaderT[F, ScoobiConfiguration, A]
  type Configured[A] = ConfiguredT[Id, A]
  def configured[A](f: ScoobiConfiguration => Id[A]): Configured[A] = Kleisli[Id, ScoobiConfiguration, A](f)

  /** type for objects depending on a Kiji instance */
  type KijiInstanceT[F[+_], A] = ReaderT[F, Kiji, A]
  type KijiInstance[A] = KijiInstanceT[Id, A]
  def withKiji[A](f: Kiji => A): KijiInstance[A] = Kleisli[Id, Kiji, A](f)

  /** Kiji rows as indexed by the entity id */
  type KijiRows = String => KijiRowData

  /** operations which can be done on a Kiji table, assuming there is a Kiji instance and an open table */
  trait KijiOperations {
    def put: (AnyRef, String, String, Any) => Unit
    def rows: KijiDataRequest => KijiRows
    def run: Unit => Unit
    def tableRun[A]: (KijiTable => A) => A
    def get: Result => Result
  }

  /** datatype encoding the sequencing of kiji operations, thanks to having a Monad instance */
  case class RunKijiOperations[A](run: KijiOperations => A)

  implicit def RunKijiOperationsMonad: Monad[RunKijiOperations] = new Monad[RunKijiOperations] {
    def point[A](a: =>A): RunKijiOperations[A] = RunKijiOperations(_ => a)
    def bind[A, B](fa: RunKijiOperations[A])(f: A => RunKijiOperations[B]): RunKijiOperations[B] = RunKijiOperations(op => f(fa.run(op)).run(op))
  }

  /**
   * open a Kiji instance and run any code needing it
   */
  def instance[A](withKiji: KijiInstance[A]): Configured[A] = configured { sc =>
    val uri = KijiURI.newBuilder(sc.configuration.get(KIJI_TEST_URI)).build
    val kiji = Kiji.Factory.open(uri, sc.configuration)
    try withKiji.run(kiji)
    finally kiji.release
  }

  /**
   * open a Kiji table and run any operations on that table
   */
  def table[A](tableName: String, layout: String)(onTable: RunKijiOperations[A]): KijiInstance[A] = withKiji { kiji =>
    if (!kiji.getTableNames.contains(tableName)) kiji.createTable(createLayout(layout).getDesc)
    val table = kiji.openTable(tableName)
    try runOnTable(onTable)(table)
    finally table.release
  }

  /** concrete implementation on the existing Kiji instance and currently opened table */
  def kijiOperations(table: KijiTable, writer: KijiTableWriter, reader: KijiTableReader) = new KijiOperations {
    def put = (id, family, qualifier, value) => writer.put(table.getEntityId(id), family, qualifier, value)
    def rows = (request: KijiDataRequest) => { (party: String) => reader.get(table.getEntityId(party) , request) }
    def run = identity
    def tableRun[T] = (f: KijiTable => T) => f(table)
    def get = identity
  }

  /** run operations on an opened table, passing in a reader and a writer */
  def runOnTable[A](onTable: RunKijiOperations[A])(table: KijiTable): A = {
    val writer = table.openTableWriter
    val reader = table.openTableReader

    try onTable.run(kijiOperations(table, writer, reader))
    finally {
      writer.close
      reader.close
    }
  }

}

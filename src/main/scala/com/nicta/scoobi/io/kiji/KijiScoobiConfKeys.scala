package com.nicta.scoobi.io.kiji

object KijiScoobiConfKeys {

  /** true/false */
  val BLOCK_CACHING_KEY = "hbase.scan.block.caching"

  /** int - number of rows to buffer before sending over the wire */
  val SERVER_PREFETCH_KEY = "hbase.scan.server.prefetch.size"
}

package com.foursquare.meadow

import com.foursquare.meadow.Implicits._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import org.bson.BSONObject

abstract class RecordAllocator[RecordType <: Record[_]] {
  def construct(): RecordType
  def destroy(rec: RecordType): Unit
}

class NormalRecordAllocator[RecordType <: Record[IdType], IdType](f: () => RecordType)
    extends RecordAllocator[RecordType] {
  def construct(): RecordType = f()
  def destroy(rec: RecordType): Unit = {}
}

class FreelistAllocator[RecordType <: Record[IdType], IdType](f: () => RecordType)
    extends RecordAllocator[RecordType] {
  private val freelist = new LinkedBlockingQueue[RecordType]()

  def construct(): RecordType = {
    val fromFreelist = freelist.poll(1, TimeUnit.NANOSECONDS)
    if (fromFreelist == null) {
      f()
    } else {
      fromFreelist
    }
  }

  def destroy(rec: RecordType): Unit = {
    rec.clearForReuse
    freelist.offer(rec)
  }
}

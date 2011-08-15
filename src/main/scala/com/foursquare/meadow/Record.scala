package com.foursquare.meadow

import org.bson.BSONObject
import scala.collection.mutable.ListMap

abstract class Record[IdType](dbo: BSONObject, newRecord: Boolean) {
  def id: IdType
  def schema: BaseSchema
  var fields: ListMap[BaseFieldDescriptor, BaseValueContainer] = new ListMap()

  def build[T, Reqd <: MaybeExists, Ext <: Extension[T]](fd: FieldDescriptor[T, Reqd, Ext]): ValueContainer[T, Reqd, Ext] = {
    val container = (
      if (newRecord) {
        fd.createForNewRecord()
      } else {
        fd.extractFrom(dbo)
      }
    )
    fields += Tuple2(fd, container)
    container
  }

  def save = synchronized {
    schema.save(this)
  }
}

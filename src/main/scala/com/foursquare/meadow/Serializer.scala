package com.foursquare.meadow

import com.foursquare.meadow.Implicits._

import com.mongodb.BasicDBObject
import java.util.Date
import net.liftweb.common.Loggable
import org.bson.types.{BSONTimestamp, BasicBSONList, ObjectId}
import org.bson.BSONObject
import org.joda.time.DateTime

abstract class Serializer[T] extends Loggable {
  def deserialize(any: Any): Option[T] = {
    if (any =? null) {
      None
    } else {
      val parsed = parseFromAny(any)
      if (!parsed.isDefined) {
        logger.error("%s: Can't parse %s/%s".format(this, any.asInstanceOf[AnyRef].getClass(), any))
      }
      parsed
    }
  }

  def parseFromAny(a: Any): Option[T]
  def serialize(t: T): Any
}

object Serializer {
  def apply[T](anything: T)(implicit m: Manifest[T]): Serializer[T] = m match {
    case _ => throw new RuntimeException("not implemented yet")
  }
}


case class ListSerializer[EltType](eltSerializer: Serializer[EltType]) extends Serializer[List[EltType]] {
  def parseFromAny(a: Any): Option[List[EltType]] = a match {
    case l: BasicBSONList => Some(
      (for (i <- 0 until l.size()) yield eltSerializer.deserialize(l.get(i))).flatMap(x => x).toList
    )
    case _ => None
  }
  def serialize(toSerialize: List[EltType]): Any = {
    val list = new BasicBSONList()
    for (elt <- toSerialize) {
      list.add(eltSerializer.serialize(elt).asInstanceOf[AnyRef])
    }
    list
  }
}
  
case object ObjectIdSerializer extends Serializer[ObjectId] {
  def parseFromAny(a: Any): Option[ObjectId] = a match {
    case oid: ObjectId => Some(oid)
    case s: String if ObjectId.isValid(s) => Some(new ObjectId(s))
    case _ => None
  }
  def serialize(t: ObjectId): Any = t
}

case object IntSerializer extends Serializer[Int] {
  def parseFromAny(a: Any): Option[Int] = a match {
    case i: Int => Some(i)
    case _ => None
  }
  def serialize(t: Int): Any = t
}

case object LongSerializer extends Serializer[Long] {
  def parseFromAny(a: Any): Option[Long] = a match {
    case l: Long => Some(l)
    case _ => None
  }
  def serialize(t: Long): Any = t
}

case object StringSerializer extends Serializer[String] {
  def parseFromAny(a: Any) = a match {
    case s: String => Some(s)
    case _ => None
  }
  def serialize(t: String): Any = t
}

case object DateTimeSerializer extends Serializer[DateTime] {
  def parseFromAny(a: Any) = a match {
    case dt: DateTime => Some(dt)
    case d: Date => Some(new DateTime(d.getTime()))
    case bsonTime: BSONTimestamp => Some(new DateTime(bsonTime.getTime()))
    case _ => None
  }
  def serialize(t: DateTime): Any = t.toDate
}

case class RecordSerializer[RecordType <: Record](recordDescriptor: RecordDescriptor[RecordType]) extends Serializer[RecordType] {
  def parseFromAny(a: Any): Option[RecordType] = a match {
    case dbo: BSONObject => Some(recordDescriptor.loadRecord(dbo))
    case _ => None
  }

  def serialize(rec: RecordType): Any = {
    recordDescriptor.serialize(rec)
  }
}

// Unsupported:
// byte[] / Binary
// Code / CodeWScope

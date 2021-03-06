package com.foursquare.meadow

import com.foursquare.meadow.Implicits._

import com.mongodb.BasicDBObject
import java.util.Date
import net.liftweb.common.Loggable
import org.bson.types.{BasicBSONList, ObjectId}
import org.bson.BSONObject
import org.joda.time.DateTime

// TODO(nsanch): should all/some of these parse NullWrapper?

/**
 * Serializer's model the transformation of a PhysicalType to a logical type
 * and vice versa. Subclasses must implement a serialize method to transform a
 * given logical type into a PhysicalType, and parsePhysicalRep to parse a
 * PhysicalType into a logical type. If either transformation isn't possible,
 * an exception will be raised, as the alternative is likely data-loss.
 */
abstract class Serializer[T] extends Loggable {
  def deserialize(any: PhysicalType): T = {
    val parsed = parsePhysicalRep(any)
    parsed.getOrElse {
      throw new RuntimeException("%s: Can't parse %s".format(this, any))
    }
  }

  def serialize(t: T): PhysicalType 
  protected def parsePhysicalRep(a: PhysicalType): Option[T]
}

case class ListSerializer[EltType](eltSerializer: Serializer[EltType]) extends Serializer[List[EltType]] {
  protected def parsePhysicalRep(a: PhysicalType): Option[List[EltType]] = a match {
    case l: BasicBSONListWrapper => Some(
      (for (i <- 0 until l.v.size()) yield {
        eltSerializer.deserialize(PhysicalType(l.v.get(i)))
      }).toList
    )
    case _ => None
  }
  def serialize(toSerialize: List[EltType]): PhysicalType = {
    val list = new BasicBSONList()
    for (elt <- toSerialize) {
      val serializedElt: PhysicalType = eltSerializer.serialize(elt)
      list.add(serializedElt.v.asInstanceOf[AnyRef])
    }
    BasicBSONListWrapper(list)
  }
}
  
case object ObjectIdSerializer extends Serializer[ObjectId] {
  protected def parsePhysicalRep(a: PhysicalType): Option[ObjectId] = a match {
    case oid: ObjectIdWrapper => Some(oid.v)
    case s: StringWrapper if ObjectId.isValid(s.v) => Some(new ObjectId(s.v))
    case _ => None
  }
  def serialize(t: ObjectId): PhysicalType = ObjectIdWrapper(t) 
}

case object BooleanSerializer extends Serializer[Boolean] {
  protected def parsePhysicalRep(a: PhysicalType): Option[Boolean] = a match {
    case b: BooleanWrapper => Some(b.v)
    case _ => None
  }
  def serialize(t: Boolean): PhysicalType = BooleanWrapper(t)
}

case object IntSerializer extends Serializer[Int] {
  protected def parsePhysicalRep(a: PhysicalType): Option[Int] = a match {
    case i: IntWrapper => Some(i.v)
    case _ => None
  }
  def serialize(t: Int): PhysicalType = IntWrapper(t)
}

case object LongSerializer extends Serializer[Long] {
  protected def parsePhysicalRep(a: PhysicalType): Option[Long] = a match {
    case l: LongWrapper => Some(l.v)
    case i: IntWrapper => Some(i.v: Long)
    case _ => None
  }
  def serialize(t: Long): PhysicalType = LongWrapper(t)
}

case object DoubleSerializer extends Serializer[Double] {
  protected def parsePhysicalRep(a: PhysicalType): Option[Double] = a match {
    case d: DoubleWrapper => Some(d.v)
    case l: LongWrapper => Some(l.v: Double)
    case i: IntWrapper => Some(i.v: Double)
    case _ => None
  }
  def serialize(t: Double): PhysicalType = DoubleWrapper(t)
}

case object StringSerializer extends Serializer[String] {
  protected def parsePhysicalRep(a: PhysicalType) = a match {
    case s: StringWrapper => Some(s.v)
    case _ => None
  }
  def serialize(t: String): PhysicalType = StringWrapper(t)
}

case object DateTimeSerializer extends Serializer[DateTime] {
  protected def parsePhysicalRep(a: PhysicalType) = a match {
    case d: DateWrapper => Some(new DateTime(d.v.getTime()))
    case _ => None
  }
  def serialize(t: DateTime): PhysicalType = DateWrapper(t.toDate)
}

case class RecordSerializer[RecordType <: Record[IdType], IdType](recordDescriptor: Schema[RecordType, IdType]) extends Serializer[RecordType] {
  protected def parsePhysicalRep(a: PhysicalType): Option[RecordType] = a match {
    case dbo: BSONObjectWrapper => Some(recordDescriptor.loadRecord(dbo.v))
    case _ => None
  }

  def serialize(rec: RecordType): PhysicalType = {
    BSONObjectWrapper(rec.serialize())
  }
}

case class MappedSerializer[T](map: Map[String, T], serializer: T => String) extends Serializer[T] {
  protected def parsePhysicalRep(a: PhysicalType): Option[T] = a match {
    case s: StringWrapper => map.get(s.v)
    case _ => None
  }

  def serialize(t: T): PhysicalType = StringWrapper(serializer(t))
}

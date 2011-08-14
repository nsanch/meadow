package com.foursquare.meadow

import com.foursquare.meadow.Implicits._

import java.util.Date
import org.joda.time.DateTime
import org.bson.types.{BSONTimestamp, BasicBSONList, ObjectId}
import org.bson.BSONObject
import org.joda.time.DateTime

abstract class PhysicalType {
  val v: Any
}
case class BooleanWrapper(override val v: Boolean) extends PhysicalType
case class DoubleWrapper(override val v: Double) extends PhysicalType
case class IntWrapper(override val v: Int) extends PhysicalType
case class LongWrapper(override val v: Long) extends PhysicalType
case class DateWrapper(override val v: Date) extends PhysicalType
case class StringWrapper(override val v: String) extends PhysicalType
case class ObjectIdWrapper(override val v: ObjectId) extends PhysicalType
case class BSONObjectWrapper(override val v: BSONObject) extends PhysicalType
case class BasicBSONListWrapper(override val v: BasicBSONList) extends PhysicalType
case object NullWrapper extends PhysicalType { override val v = null }

object PhysicalType {
  def apply(o: Any): PhysicalType = o match {
    case i: Int => IntWrapper(i)
    case l: Long => LongWrapper(l)
    case d: Double => DoubleWrapper(d)
    case d: Date => DateWrapper(d)
    case s: String => StringWrapper(s)
    case b: Boolean => BooleanWrapper(b)
    case oid: ObjectId => ObjectIdWrapper(oid)
    case bl: BasicBSONList => BasicBSONListWrapper(bl)
    case bo: BSONObject => BSONObjectWrapper(bo)
    case x: AnyRef if x =? null => NullWrapper
    case _ => throw new RuntimeException("unsupported type: " + o)
  }
}

// Unsupported:
// byte[] / Binary
// Code / CodeWScope
// Float? 
// BSONTimestamp

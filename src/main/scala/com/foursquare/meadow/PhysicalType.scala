package com.foursquare.meadow

import com.foursquare.meadow.Implicits._

import java.util.Date
import org.joda.time.DateTime
import org.bson.types.{BSONTimestamp, BasicBSONList, ObjectId}
import org.bson.BSONObject
import org.joda.time.DateTime

/*
sealed abstract class PhysicalTypeChoice
sealed abstract class PhysicalIntChoice extends PhysicalTypeChoice
sealed abstract class PhysicalLongChoice extends PhysicalTypeChoice
sealed abstract class PhysicalDateChoice extends PhysicalTypeChoice
sealed abstract class PhysicalStringChoice extends PhysicalTypeChoice
sealed abstract class PhysicalObjectIdChoice extends PhysicalTypeChoice
sealed abstract class PhysicalBSONObjectChoice extends PhysicalTypeChoice
sealed abstract class PhysicalBSONListChoice extends PhysicalTypeChoice
*/

abstract class PhysicalType {
  val v: Any
}
case class DoubleWrapper(override val v: Double) extends PhysicalType
case class IntWrapper(override val v: Int) extends PhysicalType //[PhysicalIntChoice]
case class LongWrapper(override val v: Long) extends PhysicalType //[PhysicalLongChoice]
case class DateWrapper(override val v: Date) extends PhysicalType //[PhysicalDateChoice]
case class BSONTimestampWrapper(override val v: BSONTimestamp) extends PhysicalType //[PhysicalBSONTimestampChoice]
case class StringWrapper(override val v: String) extends PhysicalType //[PhysicalStringChoice]
case class ObjectIdWrapper(override val v: ObjectId) extends PhysicalType //[PhysicalObjectIdChoice]
case class BSONObjectWrapper(override val v: BSONObject) extends PhysicalType //[PhysicalBSONObjectChoice]
case class BasicBSONListWrapper(override val v: BasicBSONList) extends PhysicalType //[PhysicalBSONListChoice]
case object NullWrapper extends PhysicalType { override val v = null }

object PhysicalType {
  def apply(o: Any): PhysicalType = o match {
    case i: Int => IntWrapper(i)
    case l: Long => LongWrapper(l)
    case d: Double => DoubleWrapper(d)
    case d: Date => DateWrapper(d)
    case bt: BSONTimestamp => BSONTimestampWrapper(bt)
    case s: String => StringWrapper(s)
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

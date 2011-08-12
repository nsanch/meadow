package com.foursquare.meadow

import com.mongodb.DBObject
import scala.reflect.Manifest

abstract class BaseDescriptor

object FieldDescriptor {
  def extractValue[T](serializer: Serializer[T], src: DBObject, name: String): Option[T] = {
    if (src.containsField(name)) {
      serializer.deserialize(src.get(name))
    } else None
  }
}

abstract class FieldDescriptor[T](val name: String, val serializer: Serializer[T]) extends BaseDescriptor {
  def this(delegate: FieldDescriptor[T]) = this(delegate.name, delegate.serializer)

  def extractFrom(src: DBObject): ValueContainer[T] = {
    new OptionalValueContainer(FieldDescriptor.extractValue(serializer,
                                                            src,
                                                            name))
  }

  def required(defaultVal: T): RequiredFieldDescriptor[T] = new RequiredFieldDescriptor(this, defaultVal)
  //def required: RequiredFieldDescriptor[T] = required(normalDefault) 
}

class RequiredFieldDescriptor[T](delegate: FieldDescriptor[T], defaultVal: T) extends FieldDescriptor[T](delegate) {
  override def extractFrom(src: DBObject): RequiredValueContainer[T] = {
    new RequiredValueContainer(FieldDescriptor.extractValue(delegate.serializer,
                                                            src,
                                                            delegate.name),
                               defaultVal)
  }
}

class ObjectIdFieldDescriptor(name: String) extends FieldDescriptor(name, ObjectIdSerializer)
class LongFieldDescriptor(name: String) extends FieldDescriptor(name, LongSerializer)
class IntFieldDescriptor(name: String) extends FieldDescriptor(name, IntSerializer)
class StringFieldDescriptor(name: String) extends FieldDescriptor(name, StringSerializer)

abstract class RecordDescriptor {
  def fields: List[BaseDescriptor]
  def create(dbo: DBObject): Record

  def load(dbo: DBObject) = create(dbo) 
}

abstract class Record(dbo: DBObject) {
  def descriptor: RecordDescriptor
}

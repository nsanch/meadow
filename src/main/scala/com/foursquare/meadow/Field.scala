package com.foursquare.meadow

import com.mongodb.DBObject

import com.foursquare.meadow.Implicits._

trait Extensions[T] {
  def onChange(oldVal: Option[T], newVal: Option[T]): Unit = ()
}
case class NoExtensions[T]() extends Extensions[T]

sealed abstract class MaybeRequired
sealed abstract class Required extends MaybeRequired
sealed abstract class NotRequired extends MaybeRequired

abstract class BaseValueContainer {
  def isDefined: Boolean
  def descriptor: BaseFieldDescriptor
  def serialize: Option[PhysicalType]
}
  
abstract class ExtendableValueContainer[T, Ext <: Extensions[T]] extends BaseValueContainer {
  // Public interface
  def getOpt: Option[T]
  def set(t: T): Unit
  def isDirty: Boolean
  def unset: Unit
  def isDefined: Boolean
  def ext: Ext
}

abstract class ValueContainer[T, Reqd <: MaybeRequired, Ext <: Extensions[T]]
    extends ExtendableValueContainer[T, Ext] {
  // Only required fields can use this method.
  def get(implicit ev: Reqd =:= Required): T
}

private[meadow] final class ConcreteValueContainer[T, Reqd <: MaybeRequired, Ext <: Extensions[T]](
    override val descriptor: FieldDescriptor[T, Reqd, Ext],
    initFrom: Option[T],
    extCreator: ExtendableValueContainer[T, Ext] => Ext,
    behaviorWhenUnset: Option[UnsetBehavior[T]]) extends ValueContainer[T, Reqd, Ext] {
  private var _origValueOpt: Option[T] = initFrom
  private var _valueOpt: Option[T] = initFrom
  private var _dirty = false

  val ext = extCreator(this)

  protected def set(newOpt: Option[T]): Unit = {
    if (newOpt !=? _valueOpt) {
      val oldOpt = _valueOpt
      _valueOpt = newOpt
      _dirty = (_valueOpt !=? _origValueOpt) 
      ext.onChange(oldOpt, _valueOpt)
    }
  }
  
  // Public interface
  override def getOpt: Option[T] = {
    if (_valueOpt.isDefined) {
      _valueOpt
    } else if (behaviorWhenUnset.isDefined) {
      behaviorWhenUnset.flatMap(_.onGetOpt(_valueOpt))
    } else {
      None
    }
  }
  override def set(t: T): Unit = set(Some(t))
  override def isDirty: Boolean = _dirty
  override def unset: Unit = set(None)
  override def isDefined: Boolean = _valueOpt.isDefined 

  override def serialize: Option[PhysicalType] = _valueOpt.map(descriptor.serializer.serialize _)

  // Only required fields can use this method.
  override def get(implicit ev: Reqd =:= Required): T = {
    // behaviorWhenUnset must be defined for any Required containers
    behaviorWhenUnset.get.onGet(_valueOpt)
  }
}

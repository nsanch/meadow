package com.foursquare.meadow

import com.mongodb.DBObject

import com.foursquare.meadow.Implicits._

sealed abstract class MaybeRequired
sealed abstract class Required extends MaybeRequired
sealed abstract class NotRequired extends MaybeRequired

abstract class BaseValueContainer {
  def isDefined: Boolean
  def descriptor: BaseFieldDescriptor
}
  
abstract class ValueContainer[T, Reqd <: MaybeRequired] extends BaseValueContainer {
  // Public interface
  def getOpt: Option[T]
  def set(t: T): Unit
  def isDirty: Boolean
  def unset: Unit
  def isDefined: Boolean

  // Only required fields can use this method.
  def get(implicit ev: Reqd =:= Required): T
}

class ConcreteValueContainer[T, Reqd <: MaybeRequired](val descriptor: FieldDescriptor[T, Reqd], 
                                                       initFrom: Option[T],
                                                       behaviorWhenUnset: Option[T] => T)
    extends ValueContainer[T, Reqd] {
  private var _origValueOpt: Option[T] = initFrom
  private var _valueOpt: Option[T] = initFrom
  private var _dirty = false

  protected def set(newOpt: Option[T]): Unit = {
    if (newOpt !=? _valueOpt) {
      _valueOpt = newOpt
      _dirty = (_valueOpt !=? _origValueOpt) 
    }
  }
  
  // Public interface
  override def getOpt: Option[T] = _valueOpt 
  override def set(t: T): Unit = set(Some(t))
  override def isDirty: Boolean = _dirty
  override def unset: Unit = set(None)
  override def isDefined: Boolean = getOpt.isDefined 

  // Only required fields can use this method.
  override def get(implicit ev: Reqd =:= Required): T = {
    behaviorWhenUnset(getOpt)
  }
}

class ValueContainerDecorator[T, Reqd <: MaybeRequired](delegate: ValueContainer[T, Reqd])
    extends ValueContainer[T, Reqd] {
  override def getOpt: Option[T] = delegate.getOpt
  override def set(t: T): Unit = delegate.set(t)
  override def isDirty: Boolean = delegate.isDirty
  override def unset: Unit = delegate.unset 
  override def isDefined: Boolean = delegate.isDefined
  override def descriptor: BaseFieldDescriptor = delegate.descriptor

  // Only required fields can use this method.
  override def get(implicit ev: Reqd =:= Required): T = delegate.get
}

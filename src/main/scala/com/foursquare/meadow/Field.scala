package com.foursquare.meadow

import com.mongodb.DBObject

import com.foursquare.meadow.Implicits._

// These phantom types are used to ensure that a FieldDescriptor doesn't let
// you specify that a field is both required and has a default value, and also
// that the 'get' method is only callable if a field is either required or has
// a default value. For more about phantom types, check out
// http://engineering.foursquare.com/2011/01/31/going-rogue-part-2-phantom-types/
sealed abstract class MaybeExists
sealed abstract class MustExist extends MaybeExists
sealed abstract class NotRequiredToExist extends MaybeExists

// These classes encapsulate two different ways that a ValueContainer may
// handle calls to 'get' and 'getOpt' when a value does not exist.
sealed abstract class UnsetBehavior[T] {
  def onGetOpt(opt: Option[T]): Option[T]
  def onGet(opt: Option[T]): T
}
case class AssertingBehavior[T]() extends UnsetBehavior[T] {
  override def onGetOpt(opt: Option[T]): Option[T] = opt
  override def onGet(opt: Option[T]): T = opt.get
}
case class DefaultValueBehavior[T](defaultVal: T) extends UnsetBehavior[T] {
  override def onGetOpt(opt: Option[T]): Option[T] = opt.orElse(Some(defaultVal))
  override def onGet(opt: Option[T]): T = opt.getOrElse(defaultVal)
}

abstract class BaseValueContainer {
  def isDefined: Boolean
  def descriptor: BaseFieldDescriptor
  def serialize: Option[PhysicalType]
}
  
abstract class ExtendableValueContainer[T, +Ext <: Extensions[T]] extends BaseValueContainer {
  // Public interface
  def getOpt: Option[T]
  def set(t: T): Unit
  def isDirty: Boolean
  def unset: Unit
  def isDefined: Boolean
  def ext: Ext
}

abstract class ValueContainer[T, Reqd <: MaybeExists, Ext <: Extensions[T]]
    extends ExtendableValueContainer[T, Ext] {
  // Only required fields can use this method.
  def get(implicit ev: Reqd =:= MustExist): T
}

private[meadow] final class ConcreteValueContainer[T, Reqd <: MaybeExists, Ext <: Extensions[T]](
    override val descriptor: FieldDescriptor[T, Reqd, Ext, _],
    initFrom: Option[T],
    extensionCreator: ExtendableValueContainer[T, Ext] => Ext,
    behaviorWhenUnset: Option[UnsetBehavior[T]]) extends ValueContainer[T, Reqd, Ext] {
  private var _origValueOpt: Option[T] = initFrom
  private var _valueOpt: Option[T] = initFrom
  private var _dirty = false

  val ext = extensionCreator(this)

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
    } else {
      behaviorWhenUnset.flatMap(_.onGetOpt(_valueOpt))
    }
  }
  override def set(t: T): Unit = set(Some(t))
  override def isDirty: Boolean = _dirty
  override def unset: Unit = set(None)
  override def isDefined: Boolean = _valueOpt.isDefined 

  override def serialize: Option[PhysicalType] = _valueOpt.map(descriptor.serializer.serialize _)

  // Only required fields can use this method.
  override def get(implicit ev: Reqd =:= MustExist): T = {
    // behaviorWhenUnset must be defined for any MustExist containers
    behaviorWhenUnset.get.onGet(_valueOpt)
  }
}

package com.foursquare.meadow

import com.mongodb.DBObject

import com.foursquare.meadow.Implicits._

abstract class ValueContainer[T](initFrom: Option[T]) {
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
  def getOpt: Option[T] = _valueOpt 
  def set(t: T): Unit = set(Some(t))
  def isDirty: Boolean = _dirty
}

class OptionalValueContainer[T](initFrom: Option[T]) extends ValueContainer[T](initFrom) {
  def unset: Unit = set(None)
  def isDefined: Boolean = getOpt.isDefined 
}

class RequiredValueContainer[T](initFrom: Option[T], defaultVal: T) extends ValueContainer(initFrom) {
  def get: T = getOpt.getOrElse(defaultVal)
}

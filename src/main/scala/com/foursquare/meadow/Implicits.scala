package com.foursquare.meadow

class WithTypeSafeEquals[T](val _anything: T) {
  def =?(t: T) = _anything == t
  def !=?(t: T) = _anything != t
}

object Implicits {
  implicit def addTypeSafeEquals[T](anything: T) = new WithTypeSafeEquals(anything)
}

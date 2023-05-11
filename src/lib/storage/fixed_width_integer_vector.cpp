#include "fixed_width_integer_vector.hpp"

namespace opossum {

template <std::unsigned_integral T>
FixedWidthIntegerVector<T>::FixedWidthIntegerVector(size_t size) : _value_ids(size) {}

template <std::unsigned_integral T>
ValueID FixedWidthIntegerVector<T>::get(const size_t index) const {
  return _value_ids.at(index);
}

template <std::unsigned_integral T>
void FixedWidthIntegerVector<T>::set(const size_t index, const ValueID value_id) {
  _value_ids.at(index) = value_id;
}

template <std::unsigned_integral T>
size_t FixedWidthIntegerVector<T>::size() const {
  return _value_ids.size();
}

template <std::unsigned_integral T>
AttributeVectorWidth FixedWidthIntegerVector<T>::width() const {
  return sizeof(T);
}

}  // namespace opossum

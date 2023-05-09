#include "value_segment.hpp"
#include <optional>

#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
ValueSegment<T>::ValueSegment(bool nullable) : _is_nullable(nullable) {}

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  if (is_null(chunk_offset)) {
    return NULL_VALUE;
  }
  return get(chunk_offset);
}

template <typename T>
bool ValueSegment<T>::is_null(const ChunkOffset chunk_offset) const {
  return is_nullable() && null_values().at(chunk_offset);
}

template <typename T>
T ValueSegment<T>::get(const ChunkOffset chunk_offset) const {
  Assert(!is_null(chunk_offset), "Tried to .get a NULL value from a ValueSegment.");
  return values().at(chunk_offset);
}

template <typename T>
std::optional<T> ValueSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  if (is_null(chunk_offset)) {
    return std::nullopt;
  }
  return get(chunk_offset);
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& value) {
  if (variant_is_null(value)) {
    Assert(is_nullable(), "Tried to append NULL value into non-nullable ValueSegment.");
    _values.emplace_back();
    _null_values.emplace_back(true);
  } else {
    try {
      _values.emplace_back(type_cast<T>(value));
    } catch (...) {
      Fail("Tried to append inconvertible value to ValueSegment.");
    }

    if (is_nullable()) {
      _null_values.emplace_back(false);
    }
  }
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return values().size();
}

template <typename T>
const std::vector<T>& ValueSegment<T>::values() const {
  return _values;
}

template <typename T>
bool ValueSegment<T>::is_nullable() const {
  return _is_nullable;
}

template <typename T>
const std::vector<bool>& ValueSegment<T>::null_values() const {
  Assert(is_nullable(), "Tried to get null_values of non-nullable ValueSegment.");
  return _null_values;
}

template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  // FIXME: From the tests, this seems to be what upstream wants. However, one could argue that the correct
  //        implementation is
  //
  //            values().capacity() * sizeof(T) + null_values().capacity() / 8
  //
  //        to bring the _null_values vector into the equation and include the additional capacity of the vectors.
  return values().size() * sizeof(T);
}

// Macro to instantiate the following classes:
// template class ValueSegment<int32_t>;
// template class ValueSegment<int64_t>;
// template class ValueSegment<float>;
// template class ValueSegment<double>;
// template class ValueSegment<std::string>;
EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum

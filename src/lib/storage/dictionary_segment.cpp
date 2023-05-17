#include <bit>
#include <map>
#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "dictionary_segment.hpp"
#include "fixed_width_integer_vector.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

namespace {

std::shared_ptr<AbstractAttributeVector> make_fitting_attribute_vector(size_t size, size_t highest_value_id) {
  const auto bits = std::bit_width(highest_value_id);
  Assert(bits <= 32, "Cannot construct FixedWidthIntegerVector for value ids with more than 32 bits.");
  if (bits <= 8) {
    return std::make_shared<FixedWidthIntegerVector<uint8_t>>(size);
  }
  if (bits <= 16) {
    return std::make_shared<FixedWidthIntegerVector<uint16_t>>(size);
  }
  return std::make_shared<FixedWidthIntegerVector<uint32_t>>(size);
}

};  // namespace

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) {
  const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(abstract_segment);
  Assert(value_segment, "Tried to create DictionarySegment<T> from abstract segment that was not ValueSegment<T>.");
  _is_nullable = value_segment->is_nullable();

  const auto& values = value_segment->values();
  auto value_to_id = std::map<T, ValueID>();
  for (auto value_index = size_t{0}, size = values.size(); value_index < size; ++value_index) {
    if (!value_segment->is_null(value_index)) {
      value_to_id.emplace(values[value_index], 0);
    }
  }

  // Start at 1 if _is_nullable, because 0 is reserved for NULL
  auto next_value_id = _is_nullable ? ValueID{1} : ValueID{0};
  for (auto& [value, value_id] : value_to_id) {
    value_id = next_value_id++;
  }

  const auto attribute_vector = make_fitting_attribute_vector(values.size(), next_value_id - 1);
  for (auto value_index = size_t{0}, size = values.size(); value_index < size; ++value_index) {
    if (_is_nullable && value_segment->is_null(value_index)) {
      attribute_vector->set(value_index, null_value_id());
    } else {
      attribute_vector->set(value_index, value_to_id.at(values[value_index]));
    }
  }

  for (auto& [value, value_id] : value_to_id) {
    // This offsets the value_id by one if _is_nullable is true, because value id 0 is reserved for NULL.
    _dictionary.emplace_back(std::move(value));
  }
  _attribute_vector = attribute_vector;
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  if (const auto optional_value = get_typed_value(chunk_offset)) {
    return *optional_value;
  }
  return NULL_VALUE;
}

template <typename T>
T DictionarySegment<T>::get(const ChunkOffset chunk_offset) const {
  const auto optional_value = get_typed_value(chunk_offset);
  Assert(optional_value.has_value(),
         "Tried to `.get` value at offset " + std::to_string(chunk_offset) + " that was NULL.");
  return *optional_value;
}

template <typename T>
std::optional<T> DictionarySegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  const auto value_id = attribute_vector()->get(chunk_offset);
  if (value_id == null_value_id()) {
    return std::nullopt;
  }

  return value_of_value_id(value_id);
}

template <typename T>
const std::vector<T>& DictionarySegment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const AbstractAttributeVector> DictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
ValueID DictionarySegment<T>::null_value_id() const {
  return _is_nullable ? ValueID{0} : INVALID_VALUE_ID;
}

template <typename T>
const T DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  Assert(_is_nullable || value_id != null_value_id(), "Tried to get value for null_value_id.");
  return dictionary().at(value_id - (_is_nullable ? 1 : 0));
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const T value) const {
  const auto lower = std::lower_bound(_dictionary.begin(), _dictionary.end(), value);
  if (lower == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }
  return static_cast<ValueID>(std::distance(_dictionary.begin(), lower) + (_is_nullable ? 1 : 0));
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  if (variant_is_null(value)) {
    Assert(_is_nullable, "DictionarySegment is not nullable.");
    return null_value_id();
  }
  try {
    return lower_bound(type_cast<T>(value));
  } catch (...) {
    Fail("Tried lower_bound for inconvertible value to DictionarySegment.");
  }
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  const auto upper = std::upper_bound(_dictionary.begin(), _dictionary.end(), value);
  if (upper == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }
  return static_cast<ValueID>(std::distance(_dictionary.begin(), upper) + (_is_nullable ? 1 : 0));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  if (variant_is_null(value)) {
    Assert(_is_nullable, "DictionarySegment is not nullable.");
    return null_value_id();
  }
  try {
    return upper_bound(type_cast<T>(value));
  } catch (...) {
    Fail("Tried upper_bound for inconvertible value to DictionarySegment.");
  }
}

template <typename T>
ChunkOffset DictionarySegment<T>::unique_values_count() const {
  return dictionary().size();
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(attribute_vector()->size());
}

template <typename T>
size_t DictionarySegment<T>::estimate_memory_usage() const {
  const auto attribute_vector_memory = attribute_vector()->size() * attribute_vector()->width();
  const auto dictionary_memory = dictionary().size() * sizeof(T);
  return attribute_vector_memory + dictionary_memory;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum

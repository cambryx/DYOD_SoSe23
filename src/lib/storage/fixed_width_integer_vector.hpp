#pragma once

#include <concepts>
#include <vector>

#include "abstract_attribute_vector.hpp"

namespace opossum {

template <std::unsigned_integral T>
class FixedWidthIntegerVector final : AbstractAttributeVector {
  explicit FixedWidthIntegerVector(size_t size);

  // Returns the value id at a given position.
  ValueID get(const size_t index) const override;

  // Sets the value id at a given position.
  void set(const size_t index, const ValueID value_id) override;

  // Returns the number of values.
  size_t size() const override;

  // Returns the width of biggest value id in bytes.
  AttributeVectorWidth width() const override;

 private:
  std::vector<T> _value_ids;
};

};  // namespace opossum

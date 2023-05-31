#include "table_scan.hpp"

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {
TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id{column_id}, _scan_type{scan_type}, _search_value(search_value) {}

namespace {
template <typename T, typename U = T>
auto predicate_for_scantype(ScanType scan_type) {
  switch (scan_type) {
    case ScanType::OpEquals:
      // Note: The unary "+" forces conversion to a function pointer - without it, the return type would be deduced to
      //       be the type of the first lambda.
      return +[](const T& lhs, const U& rhs) { return lhs == rhs; };
    case ScanType::OpNotEquals:
      return +[](const T& lhs, const U& rhs) { return lhs != rhs; };
    case ScanType::OpLessThan:
      return +[](const T& lhs, const U& rhs) { return lhs < rhs; };
    case ScanType::OpLessThanEquals:
      return +[](const T& lhs, const U& rhs) { return lhs <= rhs; };
    case ScanType::OpGreaterThan:
      return +[](const T& lhs, const U& rhs) { return lhs > rhs; };
    case ScanType::OpGreaterThanEquals:
      return +[](const T& lhs, const U& rhs) { return lhs >= rhs; };
    default:
      Fail("Invalid Scan type.");
  }
}
}  // namespace

template <typename T>
void TableScan::_scan_value_segment(const ChunkID chunk_id, const ValueSegment<T>& segment, PosList& pos_list) {
  const auto& values = segment.values();
  const auto segment_size = segment.size();
  const auto predicate = predicate_for_scantype<T>(_scan_type);
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
    if (!segment.is_null(chunk_offset) && !variant_is_null(_search_value) &&
        predicate(values[chunk_offset], get<T>(_search_value))) {
      pos_list.push_back({chunk_id, chunk_offset});
    }
  }
}

// Represents a ValueID that can be just between two integer numbers. This is used for comparing range scans of
// ValueIDs in DictionarySegments where the cutoff for the scan does not have a ValueID assigned as it is not contained
// in the dictionary.
struct InBetweenValueID {
  // clang-format off

  friend bool operator==(const ValueID& other, const InBetweenValueID& self) {
      return other == self.lower && self.is_contained();
  }
  friend bool operator!=(const ValueID& other, const InBetweenValueID& self) { return !(other == self); }
  friend bool operator<(const ValueID& other, const InBetweenValueID& self) { return other < self.lower; }
  friend bool operator<=(const ValueID& other, const InBetweenValueID& self) { return other < self.upper; }
  friend bool operator>(const ValueID& other, const InBetweenValueID& self) { return other >= self.upper; }
  friend bool operator>=(const ValueID& other, const InBetweenValueID& self) { return other >= self.lower; }

  // clang-format on

  // Whether the lower and upper bounds imply that the search value is contained in the segment.
  bool is_contained() const {
    return lower != upper;
  }

  ValueID lower, upper;
};

template <typename T>
void TableScan::_scan_dictionary_segment(const ChunkID chunk_id, const DictionarySegment<T>& segment,
                                         PosList& pos_list) {
  const auto attribute_vector = segment.attribute_vector();
  const auto predicate = predicate_for_scantype<ValueID, InBetweenValueID>(_scan_type);
  const auto segment_size = segment.size();

  // Note: If _seach_value is larger than all values in the dictionary segment, then the ids returned are
  //       INVALID_VALUE_ID - the biggest possible value of any ValueID, so the math checks out.
  const auto comparison_value =
      InBetweenValueID{segment.lower_bound(_search_value), segment.upper_bound(_search_value)};

  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
    if (predicate(attribute_vector->get(chunk_offset), comparison_value)) {
      pos_list.emplace_back(chunk_id, chunk_offset);
    }
  }
}

void TableScan::_scan_reference_segment(const ReferenceSegment& segment, PosList& pos_list) {
  const auto predicate = predicate_for_scantype<AllTypeVariant>(_scan_type);
  for (const auto row_id : *segment.pos_list()) {
    const auto value = segment.get_by_row_id(row_id);
    if (!variant_is_null(value) && predicate(value, _search_value)) {
      pos_list.emplace_back(row_id);
    }
  }
}

ColumnID TableScan::column_id() const {
  return _column_id;
}

ScanType TableScan::scan_type() const {
  return _scan_type;
}

const AllTypeVariant& TableScan::search_value() const {
  return _search_value;
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto input_table = _left_input_table();
  const auto chunk_count = input_table->chunk_count();

  // When scanning a ReferenceSegment, the newly created ReferenceSegment should not point into the former, but instead
  // refer to the underlying Table directly.
  auto referenced_table = input_table;
  auto pos_list = std::make_shared<PosList>();
  auto reference_segment_count = 0;

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(_column_id);

    resolve_data_type(input_table->column_type(_column_id), [&](auto type) {
      using Type = typename decltype(type)::type;
      const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
      const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
      const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);

      DebugAssert(value_segment || dictionary_segment || reference_segment,
                  "Segment has to be ValueSegment, DictionarySegment or ReferenceSegment.");

      if (value_segment) {
        _scan_value_segment(chunk_id, *value_segment, *pos_list);
      } else if (dictionary_segment) {
        _scan_dictionary_segment(chunk_id, *dictionary_segment, *pos_list);
      } else if (reference_segment) {
        _scan_reference_segment(*reference_segment, *pos_list);
        referenced_table = reference_segment->referenced_table();
        ++reference_segment_count;
      }
    });
  }

  Assert(reference_segment_count == 0 || (chunk_count == 1 && reference_segment_count == 1),
         "Input table for TableScan did not follow expectations about reference segment placement in chunks.");

  auto output_chunk = std::make_shared<Chunk>();

  const auto column_count = input_table->column_count();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    output_chunk->add_segment(std::make_shared<ReferenceSegment>(referenced_table, column_id, pos_list));
  }

  return std::make_shared<Table>(*input_table, std::move(output_chunk));
}
}  // namespace opossum

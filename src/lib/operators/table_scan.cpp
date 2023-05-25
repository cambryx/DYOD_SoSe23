#include "table_scan.hpp"

#include "resolve_type.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {
TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id{column_id}, _scan_type{scan_type}, _search_value(search_value) {}

template <typename T>
bool TableScan::check_scan_condition(T value) {
  try {
    const auto compare_value = type_cast<T>(_search_value);
    switch (_scan_type) {
      case ScanType::OpEquals:
        return value == compare_value;
      case ScanType::OpNotEquals:
        return value != compare_value;
      case ScanType::OpLessThan:
        return value < compare_value;
      case ScanType::OpLessThanEquals:
        return value <= compare_value;
      case ScanType::OpGreaterThan:
        return value > compare_value;
      case ScanType::OpGreaterThanEquals:
        return value >= compare_value;
    }
  } catch (...) {
    Fail("Tried comparison with incompatible type.");
  }
  return false;
}

template <typename T>
void TableScan::scan_value_segment(const ChunkID chunk_id, const ValueSegment<T>& segment, PosList& pos_list) {
  const auto& values = segment.values();
  const auto segment_size = segment.size();
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
    if (check_scan_condition(values[chunk_offset])) {
      pos_list.emplace_back(chunk_id, chunk_offset);
    }
  }
}

template <typename T>
void TableScan::scan_dictionary_segment(const ChunkID chunk_id, const DictionarySegment<T>& segment,
                                        PosList& pos_list) {}

void TableScan::scan_reference_segment(const ChunkID chunk_id, const ReferenceSegment& segment, PosList& pos_list) {}

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
  const auto table = _left_input_table();
  const auto chunk_count = table->chunk_count();

  auto pos_list = std::make_shared<PosList>();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(_column_id);

    resolve_data_type(table->column_type(_column_id), [&](auto type) {
      using Type = typename decltype(type)::type;
      const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
      const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
      const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);

      if (value_segment) {
        scan_value_segment(chunk_id, *value_segment, *pos_list);
      } else if (dictionary_segment) {
        scan_dictionary_segment(chunk_id, *dictionary_segment, *pos_list);
      } else if (reference_segment) {
        scan_reference_segment(chunk_id, *reference_segment, *pos_list);
      } else {
        Fail("Segment has to be ValueSegment, DictionarySegment or ReferenceSegment.");
      }
    });
  }

  auto output_chunk = std::make_shared<Chunk>();

  const auto column_count = table->column_count();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    output_chunk->add_segment(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
  }

  return std::make_shared<Table>(*table, std::move(output_chunk));
}
}  // namespace opossum

#pragma once

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ColumnID column_id() const;

  ScanType scan_type() const;

  const AllTypeVariant& search_value() const;

 protected:
  template <typename T>
  bool check_scan_condition(T value);
  template <typename T>
  void scan_value_segment(ChunkID chunk_id, const ValueSegment<T>& segment, PosList& pos_list);
  template <typename T>
  void scan_dictionary_segment(ChunkID chunk_id, const DictionarySegment<T>& segment, PosList& pos_list);
  void scan_reference_segment(ChunkID chunk_id, const ReferenceSegment& segment, PosList& pos_list);

  std::shared_ptr<const Table> _on_execute() override;
  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;
};

}  // namespace opossum

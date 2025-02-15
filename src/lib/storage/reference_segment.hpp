#pragma once

#include "abstract_segment.hpp"

namespace opossum {

class Table;

// ReferenceSegment is a specific segment type that stores all its values as position list of a referenced column.
class ReferenceSegment : public AbstractSegment {
 public:
  // Creates a reference segment. The parameters specify the positions and the referenced column.
  ReferenceSegment(const std::shared_ptr<const Table>& referenced_table, const ColumnID referenced_column_id,
                   const std::shared_ptr<const PosList>& pos);

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  AllTypeVariant get_by_row_id(RowID row_id) const;

  ChunkOffset size() const override;

  const std::shared_ptr<const PosList>& pos_list() const;

  const std::shared_ptr<const Table>& referenced_table() const;

  ColumnID referenced_column_id() const;

  size_t estimate_memory_usage() const final;

 protected:
  std::shared_ptr<const Table> _referenced_table;
  ColumnID _referenced_column_id;
  std::shared_ptr<const PosList> _pos_list;
};

}  // namespace opossum

#include "chunk.hpp"

#include "abstract_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(const std::shared_ptr<AbstractSegment> segment) {
  _segments.emplace_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _segments.size(), "Tried to append row with unfitting number of columns.");
  for (int index = 0, columns = column_count(); index < columns; ++index) {
    // _segment[index]->append(values[index])
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const {
  return _segments[column_id];
}

ColumnCount Chunk::column_count() const {
  return ColumnCount{_segments.size()};
}

ChunkOffset Chunk::size() const {
  if (column_count() == 0) return 0;
  return _segments.front()->size();
}

}  // namespace opossum

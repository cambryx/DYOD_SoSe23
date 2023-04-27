#include <boost/preprocessor/seq/for_each.hpp>

#include "abstract_segment.hpp"
#include "all_type_variant.hpp"
#include "chunk.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

void Chunk::add_segment(const std::shared_ptr<AbstractSegment> segment) {
  // FIXME: shared_ptr is copied each time, we could remove the `const` and would technically not change the
  //        interface, but not sure if upstream is okay with that
  _segments.emplace_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _segments.size(), "Tried to append row with unfitting number of columns.");
  for (ColumnID index{0}; const auto& segment : _segments) {
#define TRY_WITH_TYPE(r, _, type)                                              \
  do {                                                                         \
    const auto segment_ptr = dynamic_cast<ValueSegment<type>*>(segment.get()); \
    if (segment_ptr != nullptr) {                                              \
      segment_ptr->append(values[index]);                                      \
    }                                                                          \
  } while (false);

    BOOST_PP_SEQ_FOR_EACH(TRY_WITH_TYPE, _, data_types_macro);
    ++index;
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const {
  return _segments[column_id];
}

ColumnCount Chunk::column_count() const {
  return static_cast<ColumnCount>(_segments.size());
}

ChunkOffset Chunk::size() const {
  if (column_count() == 0) {
    return 0;
  }
  return _segments.front()->size();
}

}  // namespace opossum

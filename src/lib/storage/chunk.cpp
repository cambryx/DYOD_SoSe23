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

template <typename T>
static bool try_to_append_to_concrete_value_segment(AbstractSegment* segment, const AllTypeVariant& value) {
  const auto concrete_value_segment = dynamic_cast<ValueSegment<T>*>(segment);
  if (!concrete_value_segment) {
    return false;
  }
  concrete_value_segment->append(value);
  return true;
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _segments.size(), "Tried to append row with unfitting number of columns.");

  for (auto segment_index = size_t{0}, size = values.size(); segment_index < size; ++segment_index) {
    // Try all possible instantiations of `ValueSegment`, because we cannot know which subclass is inside the
    // `AbstractSegment` and still want to use the casting functionality of `ValueSegment`, that is, we cannot assume
    // that the type of `value` matches this segment.

    const auto& segment = _segments[segment_index];
    const auto& value = values[segment_index];

#define TRY_WITH_TYPE(_, __, type)                                                                 \
  {                                                                                                \
    const bool type_matched = try_to_append_to_concrete_value_segment<type>(segment.get(), value); \
    if (type_matched) {                                                                            \
      continue;                                                                                    \
    }                                                                                              \
  }
    BOOST_PP_SEQ_FOR_EACH(TRY_WITH_TYPE, _, data_types_macro);
#undef TRY_WITH_TYPE

    Fail("Could not append to chunk, because no concrete segment type was found to append to.");
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const {
  return _segments.at(column_id);
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

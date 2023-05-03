#include "table.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) : _chunk_size{target_chunk_size} {
  create_new_chunk();
}

void Table::add_column_definition(const std::string& name, const std::string& type, const bool nullable) {
  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
  _is_column_nullable.emplace_back(nullable);
}

static void add_value_segment(Chunk& chunk, const std::string& type, const bool nullable) {
  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(nullable);
    chunk.add_segment(std::move(value_segment));
  });
}

void Table::add_column(const std::string& name, const std::string& type, const bool nullable) {
  Assert(row_count() == 0, "Tried to create column on non-empty table.");
  add_column_definition(name, type, nullable);
  add_value_segment(*_chunks.back(), type, nullable);
}

void Table::create_new_chunk() {
  _chunks.emplace_back(std::make_shared<Chunk>());
  for (ColumnID id{0}; const auto& type : _column_types) {
    add_value_segment(*_chunks.back(), type, _is_column_nullable[id]);
    ++id;
  }
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() == target_chunk_size()) {
    create_new_chunk();
  }
  _chunks.back()->append(values);
}

ColumnCount Table::column_count() const {
  return _chunks.front()->column_count();
}

uint64_t Table::row_count() const {
  return (chunk_count() - 1) * target_chunk_size() + _chunks.back()->size();
}

ChunkID Table::chunk_count() const {
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  for (ColumnID id{0}; const auto& name : _column_names) {
    if (name == column_name) {
      return ColumnID{id};
    }
    ++id;
  }
  Fail("Tried to find a non-existent column.");
}

ChunkOffset Table::target_chunk_size() const {
  return _chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  return _column_names;
}

const std::string& Table::column_name(const ColumnID column_id) const {
  Assert(column_id < column_count(), "Tried invalid column bounds.");
  return _column_names[column_id];
}

const std::string& Table::column_type(const ColumnID column_id) const {
  Assert(column_id < column_count(), "Tried invalid column bounds.");
  return _column_types[column_id];
}

bool Table::column_nullable(const ColumnID column_id) const {
  Assert(column_id < column_count(), "Tried invalid column bounds.");
  return _is_column_nullable[column_id];
}

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  Assert(chunk_id < chunk_count(), "Tried invalid chunk bounds.");
  return _chunks[chunk_id];
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  Assert(chunk_id < chunk_count(), "Tried invalid chunk bounds.");
  return _chunks[chunk_id];
}

// GCOVR_EXCL_START
void Table::compress_chunk(const ChunkID chunk_id) {
  // Implementation goes here
  Fail("Implementation is missing.");
}

// GCOVR_EXCL_STOP

}  // namespace opossum

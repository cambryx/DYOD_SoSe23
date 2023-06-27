#include <atomic>
#include <thread>

#include "dictionary_segment.hpp"
#include "resolve_type.hpp"
#include "table.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

Table::Table(const ChunkOffset init_target_chunk_size) : _target_chunk_size{init_target_chunk_size}, _row_count{0} {
  create_new_chunk();
}

Table::Table(const Table& reference_table, std::shared_ptr<Chunk> single_chunk)
    : _column_names(reference_table._column_names),
      _column_types(reference_table._column_types),
      _is_column_nullable(reference_table._is_column_nullable),
      _target_chunk_size(std::numeric_limits<ChunkOffset>::max() - 1),
      _row_count(single_chunk->size()) {
  _chunks.emplace_back(std::move(single_chunk));
  _is_chunk_mutable.emplace_back(false);
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
  add_value_segment(*last_chunk(), type, nullable);
}

void Table::create_new_chunk() {
  _is_chunk_mutable.emplace_back(true);
  _chunks.emplace_back(std::make_shared<Chunk>());
  for (auto column_index = ColumnID{0}; column_index < column_count(); ++column_index) {
    add_value_segment(*last_chunk(), _column_types[column_index], _is_column_nullable[column_index]);
  }
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (!_is_chunk_mutable.back() || last_chunk()->size() == target_chunk_size()) {
    create_new_chunk();
  }
  last_chunk()->append(values);
  ++_row_count;
}

ColumnCount Table::column_count() const {
  return static_cast<ColumnCount>(_column_names.size());
}

uint64_t Table::row_count() const {
  return _row_count;
}

ChunkID Table::chunk_count() const {
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto iterator = std::ranges::find(_column_names, column_name);
  Assert(iterator != _column_names.end(), "Tried to find a non-existent column.");
  return static_cast<ColumnID>(std::distance(_column_names.begin(), iterator));
}

ChunkOffset Table::target_chunk_size() const {
  return _target_chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  return _column_names;
}

const std::string& Table::column_name(const ColumnID column_id) const {
  return _column_names.at(column_id);
}

const std::string& Table::column_type(const ColumnID column_id) const {
  return _column_types.at(column_id);
}

bool Table::column_nullable(const ColumnID column_id) const {
  return _is_column_nullable.at(column_id);
}

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  return std::atomic_load(&_chunks.at(chunk_id));
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  return std::atomic_load(&_chunks.at(chunk_id));
}

std::shared_ptr<Chunk> Table::last_chunk() {
  return get_chunk(static_cast<ChunkID>(chunk_count() - 1));
}

void Table::compress_chunk(const ChunkID chunk_id) {
  const auto chunk = get_chunk(chunk_id);
  const auto column_count = chunk->column_count();

  auto dictionary_segments = std::vector<std::shared_ptr<AbstractSegment>>(column_count);
  auto thread_handles = std::vector<std::thread>();
  thread_handles.reserve(column_count);

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    thread_handles.emplace_back([this, column_id, &chunk, &dictionary_segments]() {
      const auto segment = chunk->get_segment(column_id);
      resolve_data_type(column_type(column_id), [&](auto type) {
        using DataType = typename decltype(type)::type;
        dictionary_segments[column_id] = std::make_shared<DictionarySegment<DataType>>(segment);
      });
    });
  }

  auto compressed_chunk = std::make_shared<Chunk>();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    thread_handles[column_id].join();
    Assert(dictionary_segments[column_id], "Compression thread terminated without writing their dictionary segment.");
    compressed_chunk->add_segment(dictionary_segments[column_id]);
  }

  std::atomic_store(&_chunks.at(chunk_id), compressed_chunk);
  _is_chunk_mutable.at(chunk_id) = false;
}

}  // namespace opossum

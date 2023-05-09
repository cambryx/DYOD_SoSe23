#include "storage_manager.hpp"

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(!has_table(name), "Tried to create table with existing name.");
  _tables[name] = std::move(table);
}

void StorageManager::drop_table(const std::string& name) {
  Assert(has_table(name), "Tried to drop a non existing table.");
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  Assert(has_table(name), "Tried to get a non existing table.");
  return _tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const {
  return _tables.contains(name);
}

std::vector<std::string> StorageManager::table_names() const {
  auto table_names = std::vector<std::string>();
  table_names.reserve(_tables.size());
  for (const auto& [name, table] : _tables) {
    table_names.emplace_back(name);
  }
  std::sort(table_names.begin(), table_names.end());
  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  for (const auto& [name, table] : _tables) {
    out << "(\"" << name << "\", " << table->column_count() << " columns, " << table->row_count() << " rows, "
        << table->chunk_count() << " chunks)" << std::endl;
  }
}

void StorageManager::reset() {
  _tables.clear();
}

}  // namespace opossum

#include "get_table.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

GetTable::GetTable(const std::string& name) : _name{name} {}

const std::string& GetTable::table_name() const {
  return _name;
}

std::shared_ptr<const Table> GetTable::_on_execute() {
  return StorageManager::get().get_table(_name);
}

}  // namespace opossum

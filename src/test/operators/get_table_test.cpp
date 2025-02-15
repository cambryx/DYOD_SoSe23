#include "base_test.hpp"

#include "operators/get_table.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {
class OperatorsGetTableTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table = std::make_shared<Table>(2);
    StorageManager::get().add_table("TableA", _test_table);
  }

  std::shared_ptr<Table> _test_table;
};

TEST_F(OperatorsGetTableTest, GetOutput) {
  auto get_table_oper = std::make_shared<GetTable>("TableA");

  EXPECT_EQ(get_table_oper->get_output(), nullptr);

  get_table_oper->execute();
  EXPECT_THROW(get_table_oper->execute(), std::logic_error);  // Operators shall not be executed twice.

  EXPECT_EQ(get_table_oper->get_output(), _test_table);
}

TEST_F(OperatorsGetTableTest, GetTableName) {
  auto get_table_oper = std::make_shared<GetTable>("TableA");
  EXPECT_EQ(get_table_oper->table_name(), "TableA");
}

TEST_F(OperatorsGetTableTest, ThrowsUnknownTableName) {
  auto get_table_oper = std::make_shared<GetTable>("TableB");

  EXPECT_THROW(get_table_oper->execute(), std::logic_error) << "Should throw unknown table name exception";
}

}  // namespace opossum

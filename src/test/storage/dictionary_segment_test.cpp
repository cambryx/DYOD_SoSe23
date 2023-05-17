#include "base_test.hpp"

#include "resolve_type.hpp"
#include "storage/abstract_attribute_vector.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/dictionary_segment.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<int32_t>> value_segment_int{std::make_shared<ValueSegment<int32_t>>()};
  std::shared_ptr<ValueSegment<int64_t>> value_segment_big_int{std::make_shared<ValueSegment<int64_t>>()};
  std::shared_ptr<ValueSegment<std::string>> value_segment_str{std::make_shared<ValueSegment<std::string>>(true)};
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  value_segment_str->append("Bill");
  value_segment_str->append("Steve");
  value_segment_str->append("Alexander");
  value_segment_str->append("Steve");
  value_segment_str->append("Hasso");
  value_segment_str->append("Bill");
  value_segment_str->append(NULL_VALUE);

  const auto dict_segment = std::make_shared<DictionarySegment<std::string>>(value_segment_str);

  // Test attribute_vector size.
  EXPECT_EQ(dict_segment->size(), 7);

  // Test dictionary size (uniqueness).
  EXPECT_EQ(dict_segment->unique_values_count(), 4);

  // Test sorting.
  const auto& dict = dict_segment->dictionary();
  EXPECT_EQ(dict[0], "Alexander");
  EXPECT_EQ(dict[1], "Bill");
  EXPECT_EQ(dict[2], "Hasso");
  EXPECT_EQ(dict[3], "Steve");

  // Test NULL value handling.
  EXPECT_EQ(dict_segment->attribute_vector()->get(6), dict_segment->null_value_id());
  EXPECT_EQ(dict_segment->get_typed_value(6), std::nullopt);
  EXPECT_THROW(dict_segment->get(6), std::logic_error);
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (auto value = int16_t{0}; value <= 10; value += 2) {
    value_segment_int->append(value);
  }

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });

  auto dict_segment_int = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  value_segment_str->append("Bill");
  value_segment_str->append("Steve");
  const auto dict_segment_str = std::make_shared<DictionarySegment<std::string>>(value_segment_str);

  EXPECT_EQ(dict_segment_int->lower_bound(4), ValueID{2});
  EXPECT_EQ(dict_segment_int->upper_bound(4), ValueID{3});

  EXPECT_EQ(dict_segment_int->lower_bound(AllTypeVariant{4}), ValueID{2});
  EXPECT_EQ(dict_segment_int->upper_bound(AllTypeVariant{4}), ValueID{3});

  EXPECT_EQ(dict_segment_int->lower_bound(5), ValueID{3});
  EXPECT_EQ(dict_segment_int->upper_bound(5), ValueID{3});

  EXPECT_EQ(dict_segment_int->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment_int->upper_bound(15), INVALID_VALUE_ID);

  EXPECT_THROW(dict_segment_int->lower_bound(AllTypeVariant{"Hasso"}), std::logic_error);
  EXPECT_THROW(dict_segment_int->upper_bound(AllTypeVariant{"Opossum"}), std::logic_error);

  EXPECT_THROW(dict_segment_int->lower_bound(NULL_VALUE), std::logic_error);
  EXPECT_THROW(dict_segment_int->upper_bound(NULL_VALUE), std::logic_error);

  EXPECT_EQ(dict_segment_str->lower_bound(NULL_VALUE), dict_segment_str->null_value_id());
  EXPECT_EQ(dict_segment_str->upper_bound(NULL_VALUE), dict_segment_str->null_value_id());
}

TEST_F(StorageDictionarySegmentTest, ValueOfValueID) {
  value_segment_int->append(25);
  value_segment_int->append(100);

  value_segment_str->append("Bill");
  value_segment_str->append("Steve");

  const auto dict_segment_int = std::make_shared<DictionarySegment<int32_t>>(value_segment_int);
  const auto dict_segment_str = std::make_shared<DictionarySegment<std::string>>(value_segment_str);

  EXPECT_EQ(dict_segment_str->value_of_value_id(ValueID{1}), std::string("Bill"));
  EXPECT_EQ(dict_segment_str->value_of_value_id(ValueID{2}), std::string("Steve"));

  EXPECT_EQ(dict_segment_int->value_of_value_id(ValueID{0}), int32_t{25});
  EXPECT_EQ(dict_segment_int->value_of_value_id(ValueID{1}), int32_t{100});

  EXPECT_THROW(dict_segment_str->value_of_value_id(dict_segment_str->null_value_id()), std::logic_error);
  EXPECT_NO_THROW(dict_segment_int->value_of_value_id(dict_segment_str->null_value_id()));
}

TEST_F(StorageDictionarySegmentTest, Access) {
  value_segment_int->append(25);
  value_segment_int->append(100);

  value_segment_str->append("Bill");
  value_segment_str->append("Steve");
  value_segment_str->append(NULL_VALUE);

  EXPECT_THROW(value_segment_int->append(NULL_VALUE), std::logic_error);

  const auto dict_segment_int = std::make_shared<DictionarySegment<int32_t>>(value_segment_int);
  const auto dict_segment_str = std::make_shared<DictionarySegment<std::string>>(value_segment_str);

  EXPECT_EQ(dict_segment_int->get(0), uint8_t{25});
  EXPECT_EQ(dict_segment_str->get(0), std::string("Bill"));

  EXPECT_EQ(dict_segment_int->operator[](0), AllTypeVariant{25});
  EXPECT_EQ(dict_segment_int->operator[](1), AllTypeVariant{100});

  EXPECT_EQ(value_segment_str->operator[](0), AllTypeVariant{"Bill"});
  EXPECT_EQ(value_segment_str->operator[](1), AllTypeVariant{"Steve"});

  EXPECT_TRUE(variant_is_null(dict_segment_str->operator[](2)));
}

TEST_F(StorageDictionarySegmentTest, MemoryEstimation) {
  value_segment_big_int->append(1);
  value_segment_big_int->append(2);
  value_segment_big_int->append(3);
  value_segment_big_int->append(1);
  value_segment_big_int->append(2);
  value_segment_big_int->append(3);

  for (auto i = 0; i < 300; ++i) {
    value_segment_int->append(i);
  }

  const auto dict_segment_int = std::make_shared<DictionarySegment<int32_t>>(value_segment_int);
  const auto dict_segment_big_int = std::make_shared<DictionarySegment<int64_t>>(value_segment_big_int);

  // 6 bytes for the ValueIDs in the attribute vector (1 byte suffices for the 3 distinct values)
  // 3 distinct values with 8 byte for the uint64_t
  EXPECT_EQ(dict_segment_big_int->estimate_memory_usage(), 6 * 1 + 3 * 8);

  // 300 * 2 bytes for ValueIDs (2 bytes are needed for 300 distinct values)
  // 300 distinct values with 4 bytes for the uint32_t
  EXPECT_EQ(dict_segment_int->estimate_memory_usage(), 300 * 2 + 300 * 4);
}

TEST_F(StorageDictionarySegmentTest, AttributeVectorWidth) {
  for (auto i = 0; i < 256; ++i) {
    value_segment_big_int->append(i);
  }
  auto dict_segment_int = std::make_shared<DictionarySegment<int64_t>>(value_segment_big_int);
  EXPECT_EQ(dict_segment_int->attribute_vector()->width(), 1);

  value_segment_big_int->append(256);

  dict_segment_int = std::make_shared<DictionarySegment<int64_t>>(value_segment_big_int);
  EXPECT_EQ(dict_segment_int->attribute_vector()->width(), 2);

  for (auto i = 257; i < 65536; ++i) {
    value_segment_big_int->append(i);
  }
  dict_segment_int = std::make_shared<DictionarySegment<int64_t>>(value_segment_big_int);
  EXPECT_EQ(dict_segment_int->attribute_vector()->width(), 2);

  value_segment_big_int->append(65536);
  dict_segment_int = std::make_shared<DictionarySegment<int64_t>>(value_segment_big_int);
  EXPECT_EQ(dict_segment_int->attribute_vector()->width(), 4);

  // For nullable ValueSegments the border between different types is offset by 1 because of null_value_id
  for (auto i = 0; i < 255; ++i) {
    value_segment_str->append(i);
  }

  auto dict_segment_string = std::make_shared<DictionarySegment<std::string>>(value_segment_str);
  EXPECT_EQ(dict_segment_string->attribute_vector()->width(), 1);

  value_segment_str->append("255");
  dict_segment_string = std::make_shared<DictionarySegment<std::string>>(value_segment_str);
  EXPECT_EQ(dict_segment_string->attribute_vector()->width(), 2);
}

}  // namespace opossum

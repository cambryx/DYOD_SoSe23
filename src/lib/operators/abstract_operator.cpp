#include "abstract_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _left_input(left), _right_input(right), _was_executed{false} {}

void AbstractOperator::execute() {
  Assert(!_was_executed, "Operators shall not be executed twice.");
  _output = _on_execute();
  _was_executed = true;
}

std::shared_ptr<const Table> AbstractOperator::get_output() const {
  DebugAssert(!_was_executed || (_was_executed && _output), "Execute failed and returned nullptr.");
  return _output;
}

std::shared_ptr<const Table> AbstractOperator::_left_input_table() const {
  return _left_input->get_output();
}

std::shared_ptr<const Table> AbstractOperator::_right_input_table() const {
  return _right_input->get_output();
}

}  // namespace opossum

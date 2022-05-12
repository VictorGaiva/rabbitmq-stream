# defmodule RabbitStreamTest.Macros do
#   use ExUnit.Case
#   alias RabbitStream.Message

#   test "should generate structs" do
#     assert is_map_key(%Message.ResponseCode.Ok{}, :__struct__)
#     assert is_map_key(%Message.ResponseCode.StreamDoesNotExist{}, :__struct__)
#   end

#   test "should map the code to structs" do
#     assert Message.ResponseCode.decode(<<0x01::unsigned-integer-size(16)>>) == %Message.ResponseCode.Ok{}
#     assert Message.ResponseCode.decode(<<0x02::unsigned-integer-size(16)>>) == %Message.ResponseCode.StreamDoesNotExist{}
#   end

#   test "should decode a response" do
#     data = <<
#       0x8001::unsigned-integer-size(16), # key
#       0x02::unsigned-integer-size(16),   # version
#       0x03::unsigned-integer-size(32),   # correlation_id
#       0x04::unsigned-integer-size(16),   # response_code
#     >>

#     Message.decode(<<byte_size(data)::unsigned-integer-size(32), data::binary>>)
#   end
# end

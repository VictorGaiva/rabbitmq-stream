defmodule XColonyTest.Macros do
  use ExUnit.Case

  alias XColonyTest.MacrosHelper


  test "should generate structs" do
    assert is_map_key(%MacrosHelper.Ok{}, :__struct__)
    assert is_map_key(%MacrosHelper.StreamDoesNotExist{}, :__struct__)
  end

  test "should map the code to structs" do
    assert MacrosHelper.decode(<<0x01::unsigned-integer-size(16)>>) == %MacrosHelper.Ok{}
    assert MacrosHelper.decode(<<0x02::unsigned-integer-size(16)>>) == %MacrosHelper.StreamDoesNotExist{}
  end
end

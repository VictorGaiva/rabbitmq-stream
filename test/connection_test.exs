defmodule RabbitStreamTest.Connection do
  use ExUnit.Case
  alias RabbitStream.Connection

  test "should start link" do
    assert match?({:ok, _pid}, Connection.start_link())
  end

  test "should connect to socket" do
    {:ok, pid} = Connection.start_link()

    assert match?({:ok, _pid}, Connection.connect(pid))
  end
end

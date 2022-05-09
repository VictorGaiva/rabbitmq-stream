defmodule XColonyTest.Connection do
  use ExUnit.Case
  alias XColony.Connection

  test "should start link" do
    assert match?({:ok, _pid}, Connection.start_link())
  end

  test "should connect to socket" do
    {:ok, pid} = Connection.start_link()

    assert match?({:ok, _pid}, Connection.connect(pid))
  end

end

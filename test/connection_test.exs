defmodule RabbitStreamTest.Connection do
  use ExUnit.Case
  alias RabbitStream.Connection
  alias RabbitStream.Message.Response.Code

  test "should open and close the connection" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")

    assert match?(%Connection{state: "closed"}, Connection.get_state(pid))

    {:ok, state} = Connection.connect(pid)

    assert match?(%Connection{state: "open", host: "localhost", port: 5552, vhost: "/"}, state)

    {:ok, state} = Connection.close(pid)

    assert match?(%Connection{state: "closed", host: "localhost", port: 5552, vhost: "/"}, state)

    assert match?({:error, _}, Connection.close(pid))
  end

  test "should fail to connect with expected error message" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/NONEXISTENT")

    assert match?(%Connection{state: "closed"}, Connection.get_state(pid))

    assert match?({:error, %Code.VirtualHostAccessFailure{}}, Connection.connect(pid))
  end
end

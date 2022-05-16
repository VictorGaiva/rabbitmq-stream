defmodule RabbitStreamTest.Connection do
  use ExUnit.Case
  alias RabbitStream.Connection

  alias RabbitStream.Message.Code.{
    VirtualHostAccessFailure,
    AuthenticationFailure,
    StreamAlreadyExists,
    StreamDoesNotExist
  }

  test "should open and close the connection" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")

    assert match?(%Connection{state: "closed"}, Connection.get_state(pid))

    {:ok, state} = Connection.connect(pid)

    assert match?(%Connection{state: "open", host: "localhost", port: 5552, vhost: "/"}, state)

    {:ok, state} = Connection.close(pid)

    assert match?(%Connection{state: "closed", host: "localhost", port: 5552, vhost: "/"}, state)

    assert match?({:error, _}, Connection.close(pid))
  end

  test "should fail to connect with expected error messages" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/NONEXISTENT")

    assert match?({:error, %VirtualHostAccessFailure{}}, Connection.connect(pid))

    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/", user: "guest", password: "wrong")

    assert match?({:error, %AuthenticationFailure{}}, Connection.connect(pid))
  end

  test "should create and delete a stream" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")
    {:ok, _} = Connection.connect(pid)

    assert :ok == Connection.create_stream(pid, "test-create-01")
    assert match?({:error, %StreamAlreadyExists{}}, Connection.create_stream(pid, "test-create-01"))

    assert :ok == Connection.delete_stream(pid, "test-create-01")
    assert match?({:error, %StreamDoesNotExist{}}, Connection.delete_stream(pid, "test-create-01"))
  end

  test "should store and query an offset" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")
    {:ok, _} = Connection.connect(pid)

    Connection.delete_stream(pid, "test-store-03")
    :ok = Connection.create_stream(pid, "test-store-03")

    offset = :os.system_time(:millisecond)

    assert :ok == Connection.store_offset(pid, "test-store-03", "test-store-01", offset)

    assert match?({:ok, ^offset}, Connection.query_offset(pid, "test-store-03", "test-store-01"))

    :ok = Connection.delete_stream(pid, "test-store-03")
  end
end

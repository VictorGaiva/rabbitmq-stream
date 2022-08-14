defmodule RabbitMQStreamTest.Connection do
  use ExUnit.Case
  alias RabbitMQStream.Connection

  test "should open and close the connection" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/", lazy: true)

    assert %Connection{state: :closed} = Connection.get_state(pid)

    assert :ok = Connection.connect(pid)

    assert %Connection{state: :open} = Connection.get_state(pid)

    assert :ok = Connection.close(pid)

    assert %Connection{state: :closed} = Connection.get_state(pid)

    assert {:error, _} = Connection.close(pid)
  end

  test "should correctly answer to parallel `connect` requests" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/", lazy: true)

    result =
      Task.async_stream(0..10, fn _ -> Connection.connect(pid) end)
      |> Enum.to_list()

    assert Enum.all?(result, &({:ok, :ok} = &1))
  end

  test "should fail to connect with expected error messages" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/NONEXISTENT", lazy: true)

    assert {:error, :virtual_host_access_failure} = Connection.connect(pid)

    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/", user: "guest", password: "wrong", lazy: true)

    assert {:error, :authentication_failure} = Connection.connect(pid)
  end

  test "should create and delete a stream" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(pid)

    Connection.delete_stream(pid, "test-create-01")
    assert :ok = Connection.create_stream(pid, "test-create-01")
    assert {:error, :stream_already_exists} = Connection.create_stream(pid, "test-create-01")

    assert :ok = Connection.delete_stream(pid, "test-create-01")
    assert {:error, :stream_does_not_exist} = Connection.delete_stream(pid, "test-create-01")
    Connection.close(pid)
  end

  @stream "test-store-03"
  test "should store and query an offset" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(pid)

    Connection.delete_stream(pid, @stream)
    :ok = Connection.create_stream(pid, @stream)

    offset = :os.system_time(:millisecond)

    assert :ok = Connection.store_offset(pid, @stream, "test-store-01", offset)

    assert {:ok, ^offset} = Connection.query_offset(pid, @stream, "test-store-01")

    :ok = Connection.delete_stream(pid, @stream)
    Connection.close(pid)
  end

  @stream "test-store-04"
  test "should query stream metadata" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(pid)

    Connection.delete_stream(pid, @stream)
    :ok = Connection.create_stream(pid, @stream)

    assert {:ok, _} = Connection.query_metadata(pid, [@stream])

    :ok = Connection.delete_stream(pid, @stream)
    Connection.close(pid)
  end

  @stream "test-store-05"
  test "should declare and delete a publisher" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(pid)

    Connection.delete_stream(pid, @stream)
    :ok = Connection.create_stream(pid, @stream)

    # The publisherId sequence should always start at 1
    assert {:ok, 1} = Connection.declare_publisher(pid, @stream, "publisher-01")

    assert :ok = Connection.delete_publisher(pid, 1)

    :ok = Connection.delete_stream(pid, @stream)
    Connection.close(pid)
  end

  @stream "test-store-06"
  @publisher "publisher-02"
  test "should query publisher sequence" do
    {:ok, pid} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(pid)
    Connection.delete_stream(pid, @stream)
    :ok = Connection.create_stream(pid, @stream)
    {:ok, _} = Connection.declare_publisher(pid, @stream, @publisher)

    # Should be 0 since the publisher was just declared
    assert {:ok, 0} = Connection.query_publisher_sequence(pid, @stream, @publisher)

    :ok = Connection.delete_publisher(pid, 1)
    :ok = Connection.delete_stream(pid, @stream)
    Connection.close(pid)
  end
end

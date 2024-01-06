defmodule RabbitMQStreamTest.Connection do
  use ExUnit.Case, async: false
  alias RabbitMQStream.Connection
  import ExUnit.CaptureLog

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisedSSLConnection do
    use RabbitMQStream.Connection,
      port: 5551,
      transport: :ssl,
      ssl_opts: [
        keyfile: "services/cert/client_box_key.pem",
        certfile: "services/cert/client_box_certificate.pem",
        cacertfile: "services/cert/ca_certificate.pem",
        verify: :verify_peer
      ]
  end

  test "should open and close the connection" do
    {:ok, conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/", lazy: true)

    assert %Connection{state: :closed} = :sys.get_state(conn)

    assert :ok = SupervisedConnection.connect()

    assert %Connection{state: :open} = :sys.get_state(conn)

    assert :ok = SupervisedConnection.close()

    assert %Connection{state: :closed} = :sys.get_state(conn)

    assert :ok = SupervisedConnection.close()
  end

  test "should open and close a ssl connection" do
    {:ok, conn} = SupervisedSSLConnection.start_link(host: "localhost", vhost: "/", lazy: true)

    assert %Connection{state: :closed} = :sys.get_state(conn)

    assert :ok = SupervisedSSLConnection.connect()

    assert %Connection{state: :open} = :sys.get_state(conn)

    assert :ok = SupervisedSSLConnection.close()

    assert %Connection{state: :closed} = :sys.get_state(conn)

    assert :ok = SupervisedSSLConnection.close()
  end

  test "should correctly answer to parallel `connect` requests" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/", lazy: true)

    result =
      Task.async_stream(0..10, fn _ -> SupervisedConnection.connect() end)
      |> Enum.to_list()

    assert Enum.all?(result, &({:ok, :ok} = &1))
  end

  test "should fail to connect with expected error messages" do
    {:ok, pid} = SupervisedConnection.start_link(host: "localhost", vhost: "/NONEXISTENT", lazy: true)

    assert capture_log(fn -> assert {:error, :virtual_host_access_failure} = SupervisedConnection.connect() end) =~
             "Failed to connect"

    :ok = GenServer.stop(pid)

    {:ok, _} =
      SupervisedConnection.start_link(host: "localhost", vhost: "/", user: "guest", password: "wrong", lazy: true)

    assert capture_log(fn -> assert {:error, :authentication_failure} = SupervisedConnection.connect() end) =~
             "Failed to connect"
  end

  test "should create and delete a stream" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    SupervisedConnection.delete_stream("test-create-01")
    assert :ok = SupervisedConnection.create_stream("test-create-01")
    assert {:error, :stream_already_exists} = SupervisedConnection.create_stream("test-create-01")

    assert :ok = SupervisedConnection.delete_stream("test-create-01")
    assert {:error, :stream_does_not_exist} = SupervisedConnection.delete_stream("test-create-01")
    SupervisedConnection.close()
  end

  @stream "test-store-03"
  test "should store and query an offset" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)

    offset = :os.system_time(:millisecond)

    assert :ok = SupervisedConnection.store_offset(@stream, "test-store-01", offset)

    assert {:ok, ^offset} = SupervisedConnection.query_offset(@stream, "test-store-01")

    :ok = SupervisedConnection.delete_stream(@stream)
    SupervisedConnection.close()
  end

  @stream "test-store-04"
  test "should query stream metadata" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)

    assert {:ok, _} = SupervisedConnection.query_metadata([@stream])

    :ok = SupervisedConnection.delete_stream(@stream)
    SupervisedConnection.close()
  end

  @stream "test-store-05"
  test "should declare and delete a publisher" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)

    # The publisherId sequence should always start at 1
    assert {:ok, 1} = SupervisedConnection.declare_publisher(@stream, "publisher-01")

    assert :ok = SupervisedConnection.delete_publisher(1)

    :ok = SupervisedConnection.delete_stream(@stream)
    SupervisedConnection.close()
  end

  @stream "test-store-06"
  @publisher "publisher-02"
  test "should query publisher sequence" do
    {:ok, _} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()
    SupervisedConnection.delete_stream(@stream)
    :ok = SupervisedConnection.create_stream(@stream)
    {:ok, _} = SupervisedConnection.declare_publisher(@stream, @publisher)

    # Should be 0 since the publisher was just declared
    assert {:ok, 0} = SupervisedConnection.query_publisher_sequence(@stream, @publisher)

    :ok = SupervisedConnection.delete_publisher(1)
    :ok = SupervisedConnection.delete_stream(@stream)
    SupervisedConnection.close()
  end
end

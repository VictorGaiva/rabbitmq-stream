defmodule RabbitMQStreamTest.SuperStream do
  use ExUnit.Case, async: false
  alias RabbitMQStream.OsirisChunk
  require Logger

  defmodule SuperConsumer do
    use RabbitMQStream.SuperConsumer,
      initial_offset: :next,
      partitions: 3

    @impl true
    def handle_chunk(%OsirisChunk{data_entries: entries}, %{private: parent} = state) do
      send(parent, {:handle_chunk, entries})

      :ok
    end

    @impl true
    def before_start(_opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      state
    end

    @impl true
    def handle_update(_, true) do
      {:ok, :last}
    end
  end

  defmodule SuperProducer do
    use RabbitMQStream.SuperPublisher,
      partitions: 3
  end

  setup do
    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(conn)

    [conn: conn]
  end

  @tag :v3_13
  test "should create and delete a super_stream", %{conn: conn} do
    RabbitMQStream.Connection.delete_super_stream(conn, "invoices")

    partitions = ["invoices-0", "invoices-1", "invoices-2"]

    :ok =
      RabbitMQStream.Connection.create_super_stream(conn, "invoices", partitions, ["0", "1", "2"])

    {:ok, %{streams: streams}} = RabbitMQStream.Connection.partitions(conn, "invoices")

    assert Enum.all?(partitions, &(&1 in streams))

    :ok = RabbitMQStream.Connection.delete_super_stream(conn, "invoices")
  end

  @tag :v3_11
  @tag :v3_12
  @tag :v3_13
  test "should create super streams", %{conn: conn} do
    {:ok, _} =
      SuperConsumer.start_link(
        connection: conn,
        super_stream: "invoices",
        private: self()
      )

    {:ok, _} =
      SuperProducer.start_link(
        connection: conn,
        super_stream: "invoices"
      )

    :ok = SuperProducer.publish("1")
    :ok = SuperProducer.publish("12")
    :ok = SuperProducer.publish("123")
    Process.sleep(500)

    assert_receive {:handle_chunk, _entries} = _msg
  end
end

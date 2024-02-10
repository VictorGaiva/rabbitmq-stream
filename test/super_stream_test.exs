defmodule RabbitMQStreamTest.SuperStream do
  use ExUnit.Case, async: false
  alias RabbitMQStream.OsirisChunk
  require Logger

  defmodule SuperConsumer1 do
    use RabbitMQStream.SuperConsumer,
      initial_offset: :next,
      partitions: 3

    @impl true
    def handle_chunk(%OsirisChunk{}, %{private: parent}) do
      send(parent, __MODULE__)

      :ok
    end

    @impl true
    def handle_update(state, _) do
      {:ok, state.last_offset || state.initial_offset}
    end
  end

  defmodule SuperConsumer2 do
    use RabbitMQStream.SuperConsumer,
      initial_offset: :next,
      partitions: 3

    @impl true
    def handle_chunk(%OsirisChunk{}, %{private: parent}) do
      send(parent, __MODULE__)

      :ok
    end

    @impl true
    def handle_update(state, _) do
      {:ok, state.last_offset || state.initial_offset}
    end
  end

  defmodule SuperConsumer3 do
    use RabbitMQStream.SuperConsumer,
      initial_offset: :next,
      partitions: 3

    @impl true
    def handle_chunk(%OsirisChunk{}, %{private: parent}) do
      send(parent, __MODULE__)

      :ok
    end

    @impl true
    def handle_update(state, _) do
      {:ok, state.last_offset || state.initial_offset}
    end
  end

  defmodule SuperProducer do
    use RabbitMQStream.SuperProducer,
      partitions: 3
  end

  setup do
    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(conn)

    [conn: conn]
  end

  @tag :v3_13
  test "should create and delete a super_stream", %{conn: conn} do
    RabbitMQStream.Connection.delete_super_stream(conn, "transactions")

    :ok =
      RabbitMQStream.Connection.create_super_stream(conn, "transactions",
        "test-foo": "A",
        "test-bar": "B",
        "test-baz": "C"
      )

    {:ok, %{streams: ["test-foo"]}} = RabbitMQStream.Connection.route(conn, "A", "transactions")
    {:ok, %{streams: ["test-bar"]}} = RabbitMQStream.Connection.route(conn, "B", "transactions")
    {:ok, %{streams: ["test-baz"]}} = RabbitMQStream.Connection.route(conn, "C", "transactions")

    {:ok, %{streams: streams}} = RabbitMQStream.Connection.partitions(conn, "transactions")

    assert Enum.all?(streams, fn stream -> stream in ["test-foo", "test-bar", "test-baz"] end)

    for consumer <- [SuperConsumer1, SuperConsumer2, SuperConsumer3] do
      {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
      :ok = RabbitMQStream.Connection.connect(conn)

      {:ok, _} =
        consumer.start_link(
          connection: conn,
          super_stream: "invoices",
          private: self()
        )
    end

    # We wait a bit to guarantee that the consumers are ready
    Process.sleep(500)

    :ok = SuperProducer.publish("1")
    :ok = SuperProducer.publish("12")
    :ok = SuperProducer.publish("123")

    msgs =
      for _ <- 1..3 do
        receive do
          msg -> msg
        after
          500 -> :timeout
        end
      end

    # Process.sleep(60_000)
    assert SuperConsumer1 in msgs
    assert SuperConsumer2 in msgs
    assert SuperConsumer3 in msgs

    :ok = RabbitMQStream.Connection.delete_super_stream(conn, "transactions")
  end

  @tag :v3_11
  @tag :v3_12
  @tag :v3_13
  test "should create super streams", %{conn: conn} do
    {:ok, %{streams: streams}} =
      RabbitMQStream.Connection.query_metadata(conn, ["invoices-0", "invoices-1", "invoices-2"])

    unless Enum.all?(streams, &(&1.code == :ok)) do
      raise "SuperStream streams were not found. Please ensure you've created the \"invoices\" SuperStream with 3 partitions using RabbitMQ CLI or Management UI before running this test."
    end

    for consumer <- [SuperConsumer1, SuperConsumer2, SuperConsumer3] do
      {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
      :ok = RabbitMQStream.Connection.connect(conn)

      {:ok, _} =
        consumer.start_link(
          connection: conn,
          super_stream: "invoices",
          private: self()
        )
    end

    {:ok, _} =
      SuperProducer.start_link(
        connection: conn,
        super_stream: "invoices"
      )

    # We wait a bit to guarantee that the consumers are ready
    Process.sleep(500)

    :ok = SuperProducer.publish("1")
    :ok = SuperProducer.publish("12")
    :ok = SuperProducer.publish("123")

    msgs =
      for _ <- 1..3 do
        receive do
          msg -> msg
        end
      end

    # Process.sleep(60_000)
    assert SuperConsumer1 in msgs
    assert SuperConsumer2 in msgs
    assert SuperConsumer3 in msgs
  end
end

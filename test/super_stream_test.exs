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
    RabbitMQStream.Connection.delete_super_stream(conn, "test")

    :ok =
      RabbitMQStream.Connection.create_super_stream(conn, "test",
        "test-0": 0,
        "test-1": 1,
        "test-2": 2
      )

    {:ok, %{streams: ["test-0"]}} = RabbitMQStream.Connection.route(conn, "0", "test")
    {:ok, %{streams: ["test-1"]}} = RabbitMQStream.Connection.route(conn, "1", "test")
    {:ok, %{streams: ["test-2"]}} = RabbitMQStream.Connection.route(conn, "2", "test")

    {:ok, %{streams: streams}} = RabbitMQStream.Connection.partitions(conn, "test")

    assert Enum.all?(streams, fn stream -> stream in ["test-0", "test-1", "test-2"] end)

    :ok = RabbitMQStream.Connection.delete_super_stream(conn, "test")
  end

  @tag :v3_11
  @tag :v3_12
  @tag :v3_13
  test "should create super streams" do
    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(conn)

    {:ok, _} =
      SuperConsumer1.start_link(
        connection: conn,
        super_stream: "invoices",
        private: self()
      )

    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(conn)

    {:ok, _} =
      SuperConsumer2.start_link(
        connection: conn,
        super_stream: "invoices",
        private: self()
      )

    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(conn)

    {:ok, _} =
      SuperConsumer3.start_link(
        connection: conn,
        super_stream: "invoices",
        private: self()
      )

    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(conn)

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

defmodule RabbitMQStreamTest.Consumer.SingleActiveConsumer do
  use ExUnit.Case, async: false
  require Logger

  defmodule Conn1 do
    use RabbitMQStream.Connection
  end

  defmodule Conn2 do
    use RabbitMQStream.Connection
  end

  defmodule Conn3 do
    use RabbitMQStream.Connection
  end

  defmodule Conn4 do
    use RabbitMQStream.Connection
  end

  defmodule Publisher do
    use RabbitMQStream.Publisher,
      connection: Conn4

    @impl true
    def before_start(_opts, state) do
      state.connection.create_stream(state.stream_name)

      state
    end
  end

  defmodule Subs1 do
    use RabbitMQStream.Consumer,
      connection: Conn1,
      initial_offset: :next,
      stream_name: "super-stream-test-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [single_active_consumer: "group-1"]

    @impl true
    def handle_update(_, true) do
      {:ok, :last}
    end

    @impl true
    def handle_chunk(%{data_entries: [entry]}, %{private: parent, connection: conn}) do
      send(parent, {{conn, __MODULE__}, entry})

      :ok
    end
  end

  defmodule Subs2 do
    use RabbitMQStream.Consumer,
      connection: Conn2,
      initial_offset: :next,
      stream_name: "super-stream-test-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [single_active_consumer: "group-1"]

    @impl true
    def handle_update(_, true) do
      {:ok, :last}
    end

    @impl true
    def handle_chunk(%{data_entries: [entry]}, %{private: parent, connection: conn}) do
      send(parent, {{conn, __MODULE__}, entry})

      :ok
    end
  end

  defmodule Subs3 do
    use RabbitMQStream.Consumer,
      connection: Conn3,
      initial_offset: :next,
      stream_name: "super-stream-test-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [single_active_consumer: "group-1"]

    @impl true
    def handle_update(_, true) do
      {:ok, :last}
    end

    @impl true
    def handle_chunk(%{data_entries: [entry]}, %{private: parent, connection: conn}) do
      send(parent, {{conn, __MODULE__}, entry})

      :ok
    end
  end

  test "should always have exactly 1 active consumer" do
    {:ok, _} = Conn1.start_link(name: :conn1)
    {:ok, _} = Conn2.start_link(name: :conn2)
    {:ok, _} = Conn3.start_link(name: :conn2)
    {:ok, _} = Conn4.start_link(name: :conn2)
    :ok = Conn1.connect()
    :ok = Conn2.connect()
    :ok = Conn3.connect()
    :ok = Conn4.connect()

    :ok = Conn4.delete_stream("super-stream-test-01")

    {:ok, _} = Publisher.start_link(stream_name: "super-stream-test-01")

    {:ok, _} = Subs1.start_link(private: self())
    {:ok, _} = Subs2.start_link(private: self())
    {:ok, _} = Subs3.start_link(private: self())

    :ok = Publisher.publish("1")

    assert_receive {{_conn1, sub1}, "1"}, 500

    :ok = Publisher.publish("2")

    assert_receive {{conn1, ^sub1}, "2"}, 500

    # When calling .stop, the consumer send a 'unsubscribe' request to the connection before closing.
    # So we must first close the consumer, then the connection.
    :ok = GenServer.stop(sub1)
    :ok = GenServer.stop(conn1)

    # At this point, the newly selected consumer should have received
    # a 'consumer_update' request from the server and informed its current offset.
    # We are setting the defaults offset to ':last' in the `handle_update` callback above.
    # But a more realistic scenario would be to fetch the offset from the stream itself.
    :ok = Publisher.publish("3")

    assert_receive {{_conn, sub2}, "3"}, 500

    assert sub2 != sub1
  end
end

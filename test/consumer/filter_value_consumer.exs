defmodule RabbitMQStreamTest.Consumer.FilterValue do
  use ExUnit.Case, async: false
  require Logger

  defmodule Conn1 do
    use RabbitMQStream.Connection
  end

  defmodule Publisher do
    use RabbitMQStream.Publisher,
      connection: Conn1,
      serializer: Jason

    @impl true
    def filter_value(message) do
      message["key"]
    end
  end

  defmodule Subs1 do
    use RabbitMQStream.Subscriber,
      connection: Conn1,
      initial_offset: :next,
      stream_name: "filter-value-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [filter: ["Subs1"]],
      serializer: Jason

    @impl true
    def handle_chunk(%{data_entries: [entry]}, %{private: parent}) do
      send(parent, {__MODULE__, entry})

      :ok
    end
  end

  defmodule Subs2 do
    use RabbitMQStream.Subscriber,
      connection: Conn1,
      initial_offset: :next,
      stream_name: "filter-value-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [filter: ["Subs2"]],
      serializer: Jason

    @impl true
    def handle_chunk(%{data_entries: [entry]}, %{private: parent}) do
      send(parent, {__MODULE__, entry})

      :ok
    end
  end

  defmodule Subs3 do
    use RabbitMQStream.Subscriber,
      connection: Conn1,
      initial_offset: :next,
      stream_name: "filter-value-01",
      offset_tracking: [count: [store_after: 1]],
      properties: [match_unfiltered: true],
      serializer: Jason

    @impl true
    def handle_chunk(%{data_entries: [entry]}, %{private: parent}) do
      send(parent, {__MODULE__, entry})

      :ok
    end
  end

  test "should always have exactly 1 active consumer" do
    {:ok, _} = Conn1.start_link()
    :ok = Conn1.connect()
    Conn1.delete_stream("filter-value-01")
    :ok = Conn1.create_stream("filter-value-01")
    {:ok, _} = Publisher.start_link(stream_name: "filter-value-01")

    {:ok, _} = Subs1.start_link(private: self())
    {:ok, _} = Subs2.start_link(private: self())
    {:ok, _} = Subs3.start_link(private: self())

    message = %{"key" => "Subs1", "value" => "1"}
    :ok = Publisher.publish(message)

    assert_receive {Subs1, ^message}, 500

    message = %{"key" => "Subs2", "value" => "2"}
    :ok = Publisher.publish(message)
    assert_receive {Subs2, ^message}, 500

    message = %{"key" => "NO-FILTER-APPLIED", "value" => "3"}
    :ok = Publisher.publish(message)
    assert_receive {Subs3, ^message}, 500
  end
end

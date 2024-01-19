defmodule RabbitMQStreamTest.Consumer do
  use ExUnit.Case, async: false
  alias RabbitMQStream.OsirisChunk

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisorProducer do
    use RabbitMQStream.Producer,
      connection: SupervisedConnection

    @impl true
    def before_start(_opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      state
    end
  end

  defmodule SupervisorProducer2 do
    use RabbitMQStream.Producer,
      connection: SupervisedConnection,
      serializer: Jason

    @impl true
    def before_start(_opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      state
    end
  end

  defmodule Consumer do
    use RabbitMQStream.Consumer,
      connection: SupervisedConnection,
      serializer: Jason

    @impl true
    def handle_chunk(%OsirisChunk{data_entries: entries}, %{private: parent}) do
      send(parent, {:handle_chunk, entries})

      :ok
    end
  end

  setup do
    {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    :ok
  end

  @stream "consumer-test-stream-01"
  @reference_name "reference-01"
  test "should publish and receive a message" do
    {:ok, _producer} = SupervisorProducer.start_link(reference_name: @reference_name, stream_name: @stream)

    assert {:ok, subscription_id} = SupervisedConnection.subscribe(@stream, self(), :next, 999)

    message = Jason.encode!(%{message: "Hello, world2!"})

    SupervisorProducer.publish(message)

    assert_receive {:chunk, %OsirisChunk{data_entries: [^message]}}, 500

    assert :ok = SupervisedConnection.unsubscribe(subscription_id)

    SupervisorProducer.publish(message)

    refute_receive {:chunk, %OsirisChunk{}}, 500
    SupervisedConnection.delete_stream(@stream)
  end

  @stream "consumer-test-stream-02"
  @reference_name "reference-02"
  test "should credit a consumer" do
    {:ok, _producer} = SupervisorProducer.start_link(reference_name: @reference_name, stream_name: @stream)

    # We ensure the stream exists before consuming
    SupervisedConnection.create_stream(@stream)
    assert {:ok, subscription_id} = SupervisedConnection.subscribe(@stream, self(), :next, 1)

    message = Jason.encode!(%{message: "Hello, world!"})

    SupervisorProducer.publish(message)

    assert_receive {:chunk, %OsirisChunk{data_entries: [^message]}}, 500

    message = Jason.encode!(%{message: "Hello, world2!"})

    SupervisorProducer.publish(message)

    refute_receive {:chunk, %OsirisChunk{}}, 500

    assert :ok = SupervisedConnection.credit(subscription_id, 1)

    assert_receive {:chunk, %OsirisChunk{data_entries: [^message]}}, 500
    SupervisedConnection.delete_stream(@stream)
  end

  @stream "consumer-test-stream-10"
  @reference_name "reference-10"
  test "a message should be received by a persistent consumer" do
    SupervisedConnection.delete_stream(@stream)

    {:ok, _producer} =
      SupervisorProducer2.start_link(reference_name: @reference_name, stream_name: @stream)

    {:ok, _subscriber} =
      Consumer.start_link(
        initial_offset: :next,
        stream_name: @stream,
        private: self(),
        offset_tracking: [count: [store_after: 1]]
      )

    message1 = %{"message" => "Consumer Test: 1"}
    message2 = %{"message" => "Consumer Test: 2"}

    SupervisorProducer2.publish(message1)
    assert_receive {:handle_chunk, [^message1]}, 500

    SupervisorProducer2.publish(message2)
    assert_receive {:handle_chunk, [^message2]}, 500

    :ok = GenServer.stop(Consumer, :normal)

    {:ok, _subscriber} =
      Consumer.start_link(
        initial_offset: :next,
        stream_name: @stream,
        private: self(),
        offset_tracking: [count: [store_after: 1]]
      )

    assert_receive {:handle_chunk, [^message2]}, 500

    SupervisedConnection.delete_stream(@stream)
  end
end

defmodule RabbitMQStreamTest.Subscriber do
  use ExUnit.Case, async: false
  alias RabbitMQStream.Message.Data.DeliverData

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisorPublisher do
    use RabbitMQStream.Publisher,
      connection: RabbitMQStreamTest.Subscriber.SupervisedConnection
  end

  setup do
    {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    :ok
  end

  @stream "stream-01"
  @reference_name "reference-01"
  test "should publish and receive a message" do
    {:ok, _publisher} = SupervisorPublisher.start_link(reference_name: @reference_name, stream_name: @stream)

    assert {:ok, subscription_id} = SupervisedConnection.subscribe(@stream, self(), :next, 999)

    message = inspect(%{message: "Hello, world2!"})

    SupervisorPublisher.publish(message)

    assert_receive {:message, %DeliverData{osiris_chunk: %{data_entries: [^message]}}}, 1_000

    assert :ok = SupervisedConnection.unsubscribe(subscription_id)

    SupervisorPublisher.publish(message)

    refute_receive {:message, %DeliverData{}}, 1_000
  end
end

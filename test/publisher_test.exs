defmodule RabbitMQStreamTest.Publisher do
  use ExUnit.Case, async: false

  alias RabbitMQStream.Connection

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisorPublisher do
    use RabbitMQStream.Publisher,
      stream_name: "stream-00",
      connection: RabbitMQStreamTest.Publisher.SupervisedConnection
  end

  setup do
    {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    :ok
  end

  @stream "stream-01"
  @reference_name "reference-01"
  test "should declare itself and its stream" do
    assert {:ok, _} =
             SupervisorPublisher.start_link(
               reference_name: @reference_name,
               stream_name: @stream
             )

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "stream-02"
  @reference_name "reference-02"
  test "should query its sequence when declaring" do
    {:ok, _} =
      SupervisorPublisher.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    SupervisedConnection.delete_stream(@stream)
    assert %{sequence: 1} = SupervisorPublisher.get_state()
  end

  @stream "stream-03"
  @reference_name "reference-03"
  test "should publish a message" do
    {:ok, _} =
      SupervisorPublisher.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    %{sequence: sequence} = SupervisorPublisher.get_state()

    SupervisorPublisher.publish(inspect(%{message: "Hello, world!"}))

    sequence = sequence + 1

    assert %{sequence: ^sequence} = SupervisorPublisher.get_state()

    SupervisorPublisher.publish(inspect(%{message: "Hello, world2!"}))

    sequence = sequence + 1

    assert %{sequence: ^sequence} = SupervisorPublisher.get_state()

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "stream-04"
  @reference_name "reference-04"
  test "should keep track of sequence across startups" do
    {:ok, _} =
      SupervisorPublisher.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    SupervisorPublisher.publish(inspect(%{message: "Hello, world!"}))
    SupervisorPublisher.publish(inspect(%{message: "Hello, world2!"}))

    %{sequence: sequence} = SupervisorPublisher.get_state()

    assert :ok = SupervisorPublisher.stop()

    {:ok, _} =
      SupervisorPublisher.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    assert %{sequence: ^sequence} = SupervisorPublisher.get_state()

    SupervisedConnection.delete_stream(@stream)
  end
end

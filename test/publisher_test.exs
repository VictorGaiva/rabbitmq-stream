defmodule RabbitMQStreamTest.Publisher do
  use ExUnit.Case, async: false

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisorPublisher do
    use RabbitMQStream.Publisher,
      connection: RabbitMQStreamTest.Publisher.SupervisedConnection

    @impl true
    def before_start(_opts, state) do
      RabbitMQStream.Connection.create_stream(state.connection, state.stream_name)

      state
    end
  end

  setup do
    {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()

    :ok
  end

  @stream "publisher-test-01"
  @reference_name "publisher-test-reference-01"
  test "should declare itself and its stream" do
    assert {:ok, _} =
             SupervisorPublisher.start_link(
               reference_name: @reference_name,
               stream_name: @stream
             )

    SupervisedConnection.delete_stream(@stream)
  end

  @stream "publisher-test-02"
  @reference_name "publisher-test-reference-02"
  test "should query its sequence when declaring" do
    {:ok, _} =
      SupervisorPublisher.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    assert %{sequence: 1} = :sys.get_state(Process.whereis(SupervisorPublisher))
    SupervisedConnection.delete_stream(@stream)
  end

  @stream "publisher-test-03"
  @reference_name "publisher-test-reference-03"
  test "should publish a message" do
    {:ok, _} =
      SupervisorPublisher.start_link(
        reference_name: @reference_name,
        stream_name: @stream
      )

    %{sequence: sequence} = :sys.get_state(Process.whereis(SupervisorPublisher))

    SupervisorPublisher.publish(inspect(%{message: "Hello, world!"}))

    sequence = sequence + 1

    assert %{sequence: ^sequence} = :sys.get_state(Process.whereis(SupervisorPublisher))

    SupervisorPublisher.publish(inspect(%{message: "Hello, world2!"}))

    sequence = sequence + 1

    assert %{sequence: ^sequence} = :sys.get_state(Process.whereis(SupervisorPublisher))

    SupervisedConnection.delete_stream(@stream)
  end
end

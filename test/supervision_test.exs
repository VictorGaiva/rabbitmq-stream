defmodule RabbitMQStreamTest.Supervision do
  use ExUnit.Case, async: false

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SupervisorPublisher do
    use RabbitMQStream.Publisher,
      connection: RabbitMQStreamTest.Supervision.SupervisedConnection
  end

  defmodule MySupervisor do
    use Supervisor

    def start_link do
      Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    @stream "stream-08"
    @reference_name "reference-08"

    def init(:ok) do
      children = [
        RabbitMQStreamTest.Supervision.SupervisedConnection,
        {RabbitMQStreamTest.Supervision.SupervisorPublisher, reference_name: @reference_name, stream_name: @stream}
      ]

      Supervisor.init(children, strategy: :one_for_all)
    end
  end

  @stream "stream-01"
  @reference_name "reference-01"

  test "should start itself and publish a message" do
    {:ok, _conn} = SupervisedConnection.start_link(host: "localhost", vhost: "/")
    :ok = SupervisedConnection.connect()
    assert {:ok, _publisher} = SupervisorPublisher.start_link(reference_name: @reference_name, stream_name: @stream)

    %{sequence: sequence} = SupervisorPublisher.get_state()

    assert :ok = SupervisorPublisher.publish("Hello, world!")

    sequence = sequence + 1

    assert %{sequence: ^sequence} = SupervisorPublisher.get_state()
  end

  test "should start itself and publish a message via supervisor" do
    {:ok, _supervisor} = MySupervisor.start_link()
    assert :ok = SupervisorPublisher.publish("Hello, world!")
  end
end

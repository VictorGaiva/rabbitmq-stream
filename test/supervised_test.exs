defmodule RabbitMQStreamTest.SupervisedPublisher do
  use ExUnit.Case
  alias RabbitMQStream.{SupervisedPublisher}

  defmodule SupervisorTest do
    use SupervisedPublisher,
      reference_name: "SupervisedPublisher",
      stream_name: "supervised-stream",
      connection: [
        host: "localhost",
        vhost: "/"
      ]
  end

  test "should start itself and publish a message" do
    {:ok, _supervised} = SupervisorTest.start_link(host: "localhost", vhost: "/")

    %{sequence: sequence} = SupervisorTest.get_publisher_state()

    assert :ok == SupervisorTest.publish("Hello, world!")

    sequence = sequence + 1

    assert match?(%{sequence: ^sequence}, SupervisorTest.get_publisher_state())
  end
end

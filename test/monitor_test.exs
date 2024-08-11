defmodule RabbitMQStreamTest.Monitor do
  use ExUnit.Case, async: false

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13

  test "should monitor state transitions" do
    {:ok, conn1} = RabbitMQStream.Connection.start_link(lazy: true, host: "rabbitmq1")

    :ok = RabbitMQStream.Connection.monitor(conn1, self())

    :ok = RabbitMQStream.Connection.connect(conn1)

    assert :ok =
             (receive do
                {:rabbitmq_stream, :connection, :transition, {:closed, :connecting}, ^conn1} ->
                  :ok
              end)

    assert :ok =
             (receive do
                {:rabbitmq_stream, :connection, :transition, {:connecting, :opening}, ^conn1} ->
                  :ok
              end)

    assert :ok =
             (receive do
                {:rabbitmq_stream, :connection, :transition, {:opening, :open}, ^conn1} ->
                  :ok
              end)
  end
end

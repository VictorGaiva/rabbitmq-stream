defmodule RabbitMQStreamTest.Monitor do
  use ExUnit.Case, async: false

  @moduletag :v3_11
  @moduletag :v3_12
  @moduletag :v3_13

  test "should monitor state transitions" do
    {:ok, conn1} = RabbitMQStream.Connection.start_link(lazy: true, host: "rabbitmq1")

    {:ok, ref} = RabbitMQStream.Connection.monitor(conn1)

    :ok = RabbitMQStream.Connection.connect(conn1)

    assert :ok =
             (receive do
                {:rabbitmq_stream, ^ref, :connection, {:transition, :closed, :connecting}, ^conn1, _} ->
                  :ok
              end)

    assert :ok =
             (receive do
                {:rabbitmq_stream, ^ref, :connection, {:transition, :connecting, :opening}, ^conn1, _} ->
                  :ok
              end)

    assert :ok =
             (receive do
                {:rabbitmq_stream, ^ref, :connection, {:transition, :opening, :open}, ^conn1, _} ->
                  :ok
              end)
  end
end

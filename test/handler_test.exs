defmodule RabbitMQStreamTest.Handler do
  use ExUnit.Case, async: true
  alias RabbitMQStream.Connection

  alias RabbitMQStream.Message.Types
  alias RabbitMQStream.Message.Response

  test "after opening, it should exchange the commands versions" do
    {:ok, conn} = Connection.Client.init(host: "localhost", vhost: "/", lazy: true)

    conn =
      conn
      |> Map.put(:state, :opening)
      |> Connection.Handler.handle_message(%Response{command: :open, data: %Types.OpenData{}})

    assert {:value, {:request, :exchange_command_versions, _}} = :queue.peek(conn.commands_buffer)
  end
end

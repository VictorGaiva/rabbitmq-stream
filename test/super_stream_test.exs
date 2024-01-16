defmodule RabbitMQStreamTest.SuperStream do
  use ExUnit.Case, async: false
  alias RabbitMQStream.OsirisChunk
  require Logger

  @moduletag :v3_13

  defmodule SuperConsumer do
    use RabbitMQStream.SuperConsumer

    @impl true
    def handle_chunk(%OsirisChunk{data_entries: entries}, %{private: parent}) do
      send(parent, {:handle_chunk, entries})

      :ok
    end
  end

  setup do
    {:ok, conn} = RabbitMQStream.Connection.start_link(host: "localhost", vhost: "/")
    :ok = RabbitMQStream.Connection.connect(conn)

    [conn: conn]
  end

  test "should create and delete a super_stream", %{conn: conn} do
    RabbitMQStream.Connection.delete_super_stream(conn, "invoices")

    partitions = ["invoices-0", "invoices-1", "invoices-2"]

    :ok =
      RabbitMQStream.Connection.create_super_stream(conn, "invoices", partitions, ["0", "1", "2"])

    {:ok, %{streams: streams}} = RabbitMQStream.Connection.partitions(conn, "invoices")

    assert Enum.all?(partitions, &(&1 in streams))

    :ok = RabbitMQStream.Connection.delete_super_stream(conn, "invoices")
  end

  @stream "super-streams-01"
  test "should create super streams", %{conn: conn} do
    {:ok, _} =
      SuperConsumer.start_link(
        connection: conn,
        super_stream: @stream,
        partitions: ["01", "02", "03"],
        consumer_opts: []
      )

    Process.sleep(500)
  end
end

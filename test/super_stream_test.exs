defmodule RabbitMQStreamTest.SuperStream do
  use ExUnit.Case, async: false
  alias RabbitMQStream.OsirisChunk
  require Logger

  defmodule SupervisedConnection do
    use RabbitMQStream.Connection
  end

  defmodule SuperConsumer do
    use RabbitMQStream.SuperConsumer,
      connection: SupervisedConnection

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

  @stream "super-streams-01"
  test "should create super streams" do
    {:ok, _} =
      SuperConsumer.start_link(
        super_stream: @stream,
        partitions: ["01", "02", "03"],
        connection: SupervisedConnection,
        consumer_opts: []
      )

    Process.sleep(500)
  end
end

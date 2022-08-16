defmodule RabbitMQStreamTest.Subscriber do
  use ExUnit.Case
  alias RabbitMQStream.{Connection, Publisher}
  alias RabbitMQStream.Helpers.OsirisChunk

  @stream "stream-01"
  @reference_name "reference-01"
  test "should declare itself and its stream" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)

    {:ok, publisher} = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    assert {:ok, _} = Connection.subscribe(conn, @stream, self(), :next, 999)

    message = inspect(%{message: "Hello, world2!"})

    Publisher.publish(publisher, message)

    assert_receive {:message, %OsirisChunk{data_entries: [^message]}}, 20_000
  end
end

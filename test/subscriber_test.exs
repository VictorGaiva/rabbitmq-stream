defmodule RabbitMQStreamTest.Subscriber do
  use ExUnit.Case
  alias RabbitMQStream.{Connection, Publisher}

  @stream "stream-01"
  @reference_name "reference-01"
  test "should declare itself and its stream" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)

    {:ok, publisher} = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    assert {:ok, subscription_id} = Connection.subscribe(conn, @stream, self(), :next, 999)

    Publisher.publish(publisher, inspect(%{message: "Hello, world2!"}))

    assert_receive {:message, _}, 20_000
  end
end

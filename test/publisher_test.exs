defmodule RabbitStreamTest.Publisher do
  use ExUnit.Case
  alias RabbitStream.{Connection, Publisher}

  @stream "test-stream"
  @reference_name "test-stream"
  test "should declare itself" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)

    Connection.delete_stream(conn, @stream)
    :ok = Connection.create_stream(conn, @stream)

    result = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    assert match?({:ok, _}, result)

    {:ok, publisher} = result

    assert match?(%{connection: ^conn}, Publisher.get_state(publisher))
  end
end

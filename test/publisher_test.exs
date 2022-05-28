defmodule RabbitStreamTest.Publisher do
  use ExUnit.Case
  alias RabbitStream.{Connection, Publisher}

  @stream "stream-01"
  @reference_name "reference-01"
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

  @stream "stream-02"
  @reference_name "reference-02"
  test "should query its sequence when declaring" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)
    Connection.delete_stream(conn, @stream)
    :ok = Connection.create_stream(conn, @stream)

    {:ok, publisher} = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    assert match?(%{sequence: 1}, Publisher.get_state(publisher))
  end

  @stream "stream-03"
  @reference_name "reference-03"
  test "should publish a message" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)
    Connection.delete_stream(conn, @stream)
    Connection.create_stream(conn, @stream)

    {:ok, publisher} = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    %{sequence: sequence} = Publisher.get_state(publisher)

    # Publishing a message should increase the sequence by one
    Publisher.publish(publisher, inspect(%{message: "Hello, world!"}))

    sequence = sequence + 1

    assert match?(%{sequence: ^sequence}, Publisher.get_state(publisher))

    Publisher.publish(publisher, inspect(%{message: "Hello, world2!"}))

    sequence = sequence + 1

    assert match?(%{sequence: ^sequence}, Publisher.get_state(publisher))

    Connection.delete_stream(conn, @stream)
    Connection.close(conn, @stream)
  end
end

defmodule RabbitMQStreamTest.Publisher do
  use ExUnit.Case
  alias RabbitMQStream.{Connection, Publisher}

  defmodule SupervisorTest do
    use Publisher,
      stream_name: "stream-00"
  end

  @stream "stream-01"
  @reference_name "reference-01"
  test "should declare itself and its stream" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)

    assert {:ok, publisher} =
             Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    Connection.delete_stream(conn, @stream)
    assert %{connection: ^conn} = Publisher.get_state(publisher)
  end

  @stream "stream-02"
  @reference_name "reference-02"
  test "should query its sequence when declaring" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)

    {:ok, publisher} = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    Connection.delete_stream(conn, @stream)
    assert %{sequence: 1} = Publisher.get_state(publisher)
  end

  @stream "stream-03"
  @reference_name "reference-03"
  test "should publish a message" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)

    {:ok, publisher} = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    %{sequence: sequence} = Publisher.get_state(publisher)

    Publisher.publish(publisher, inspect(%{message: "Hello, world!"}))

    sequence = sequence + 1

    assert %{sequence: ^sequence} = Publisher.get_state(publisher)

    Publisher.publish(publisher, inspect(%{message: "Hello, world2!"}))

    sequence = sequence + 1

    assert %{sequence: ^sequence} = Publisher.get_state(publisher)

    Connection.delete_stream(conn, @stream)
    Connection.close(conn, @stream)
  end

  @stream "stream-04"
  @reference_name "reference-04"
  test "should keep track of sequence across startups" do
    {:ok, conn} = Connection.start_link(host: "localhost", vhost: "/")
    :ok = Connection.connect(conn)

    {:ok, publisher} = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    Publisher.publish(publisher, inspect(%{message: "Hello, world!"}))
    Publisher.publish(publisher, inspect(%{message: "Hello, world2!"}))

    %{sequence: sequence} = Publisher.get_state(publisher)

    assert :ok = Publisher.stop(publisher)

    {:ok, publisher} = Publisher.start_link(connection: conn, reference_name: @reference_name, stream_name: @stream)

    assert %{sequence: ^sequence} = Publisher.get_state(publisher)

    Connection.delete_stream(conn, @stream)
    Connection.close(conn, @stream)
  end

  test "should start itself and publish a message" do
    {:ok, _supervised} = SupervisorTest.start_link(host: "localhost", vhost: "/")

    %{sequence: sequence} = SupervisorTest.get_publisher_state()

    assert :ok = SupervisorTest.publish("Hello, world!")

    sequence = sequence + 1

    assert %{sequence: ^sequence} = SupervisorTest.get_publisher_state()
  end
end

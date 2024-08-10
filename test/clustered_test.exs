defmodule RabbitMQStreamTest.Clustered do
  use ExUnit.Case, async: false

  @tag :v3_13_proxied_cluster
  test "should auto discover and connect to all node when behind a loadbalancer" do
  end

  @tag :v3_13_cluster
  test "should auto discover and connect to all nodes" do
    {:ok, conn1} = RabbitMQStream.Connection.start_link(host: "rabbitmq1")
    {:ok, conn2} = RabbitMQStream.Connection.start_link(host: "rabbitmq2")
    {:ok, conn3} = RabbitMQStream.Connection.start_link(host: "rabbitmq3")

    assert :ok = RabbitMQStream.Connection.connect(conn1)
    assert :ok = RabbitMQStream.Connection.connect(conn2)
    assert :ok = RabbitMQStream.Connection.connect(conn3)

    RabbitMQStream.Connection.create_stream(conn1, "stream1")
    RabbitMQStream.Connection.create_stream(conn2, "stream2")
    RabbitMQStream.Connection.create_stream(conn3, "stream3")

    dbg(RabbitMQStream.Connection.query_metadata(conn1, ["stream1", "stream2", "stream3"]))

    dbg(:sys.get_state(conn1))
    dbg(:sys.get_state(conn2))
    dbg(:sys.get_state(conn3))
  end
end

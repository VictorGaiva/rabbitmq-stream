defmodule RabbitMQStream.SuperStream.Helpers do
  @moduledoc false
  def get_partitions(connection, super_stream, partitions) do
    case RabbitMQStream.Connection.partitions(connection, super_stream) do
      {:error, :unsupported} ->
        for routing_key <- 0..(partitions - 1) do
          "#{super_stream}-#{routing_key}"
        end

      {:ok, %{streams: streams}} ->
        streams
    end
  end

  def get_routes(connection, super_stream, routing_key) do
    if RabbitMQStream.Connection.supports?(connection, :route) do
      with {:ok, data} <- RabbitMQStream.Connection.route(connection, routing_key, super_stream) do
        data.streams
      end
    else
      ["#{super_stream}-#{routing_key}"]
    end
  end
end

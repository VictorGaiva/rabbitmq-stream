defmodule RabbitMQStream.SuperProducer.Behaviour do
  @doc """
  Callback responsible for generating the routing key for a given message and
  partitions size, which is then used to forward the publish request to the
  RabbitMQStream.Producer process responsible for the partition.

  """
  @callback routing_key(message :: binary(), partitions :: non_neg_integer()) :: non_neg_integer() | binary()
end

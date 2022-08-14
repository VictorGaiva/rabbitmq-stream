defmodule RabbitMQStream.Message.Response do
  @moduledoc false
  require Logger

  alias __MODULE__

  alias RabbitMQStream.{Connection, Message}

  defstruct([
    :version,
    :command,
    :correlation_id,
    :data,
    :code
  ])

  def new!(%Connection{} = conn, :tune, correlation_id: correlation_id) do
    :rabbit_stream_core.frame({
      :response,
      correlation_id,
      {
        :tune,
        conn.options[:frame_max],
        conn.options[:heartbeat]
      }
    })
  end

  def new!(%Connection{} = conn, :heartbeat, _) do
    :rabbit_stream_core.frame({:heartbeat})
  end

  def new!(%Connection{} = conn, :close, correlation_id: correlation_id, code: code) do
    :rabbit_stream_core.frame({
      :response,
      correlation_id,
      {
        :close,
        code
      }
    })
  end
end

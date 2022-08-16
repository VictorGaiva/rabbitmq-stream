defmodule RabbitMQStream.Message.Response do
  @moduledoc false
  require Logger

  alias RabbitMQStream.Connection

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

  def new!(%Connection{}, :heartbeat, _) do
    :rabbit_stream_core.frame(:heartbeat)
  end

  def new!(%Connection{}, :close, correlation_id: correlation_id, code: code) do
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

defmodule RabbitMQStream.Message.Response do
  @moduledoc false
  require Logger

  alias __MODULE__

  alias RabbitMQStream.{Connection, Message}

  alias Message.Data.{
    TuneData,
    CloseData,
    HeartbeatData
  }

  defstruct([
    :version,
    :command,
    :correlation_id,
    :data,
    :code
  ])

  def new!(%Connection{} = conn, :tune, correlation_id: correlation_id) do
    %Response{
      version: conn.version,
      command: :tune,
      correlation_id: correlation_id,
      data: %TuneData{
        frame_max: conn.frame_max,
        heartbeat: conn.heartbeat
      }
    }
  end

  def new!(%Connection{} = conn, :heartbeat, correlation_id: correlation_id) do
    %Response{
      version: conn.version,
      command: :heartbeat,
      correlation_id: correlation_id,
      data: %HeartbeatData{}
    }
  end

  def new!(%Connection{} = conn, :close, correlation_id: correlation_id, code: code) do
    %Response{
      version: conn.version,
      correlation_id: correlation_id,
      command: :close,
      data: %CloseData{},
      code: code
    }
  end
end

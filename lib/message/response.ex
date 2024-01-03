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

  def new!(%Connection{options: options}, :tune, correlation_id: correlation_id) do
    %Response{
      version: 1,
      command: :tune,
      correlation_id: correlation_id,
      data: %TuneData{
        frame_max: options[:frame_max],
        heartbeat: options[:heartbeat]
      }
    }
  end

  def new!(%Connection{}, :heartbeat, correlation_id: correlation_id) do
    %Response{
      version: 1,
      command: :heartbeat,
      correlation_id: correlation_id,
      data: %HeartbeatData{}
    }
  end

  def new!(%Connection{}, :close, correlation_id: correlation_id, code: code) do
    %Response{
      version: 1,
      correlation_id: correlation_id,
      command: :close,
      data: %CloseData{},
      code: code
    }
  end
end

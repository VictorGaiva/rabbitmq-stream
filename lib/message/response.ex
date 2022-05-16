defmodule RabbitStream.Message.Response do
  alias RabbitStream.Message.{Response, Command, Encoder}
  alias RabbitStream.Connection
  require Logger

  alias Command.Code.{
    Tune,
    Close,
    Heartbeat
  }

  alias RabbitStream.Message.Data.{
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
      command: %Tune{},
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
      command: %Heartbeat{},
      correlation_id: correlation_id,
      data: %HeartbeatData{}
    }
  end

  def new!(%Connection{} = conn, :close,
        correlation_id: correlation_id,
        code: code
      ) do
    %Response{
      version: conn.version,
      correlation_id: correlation_id,
      command: %Close{},
      data: %CloseData{},
      code: code
    }
  end

  def new_encoded!(%Connection{} = conn, command, opts) do
    conn
    |> new!(command, opts)
    |> Encoder.encode!()
  end
end

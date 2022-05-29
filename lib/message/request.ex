defmodule RabbitMQStream.Message.Request do
  @moduledoc false
  require Logger
  alias __MODULE__

  alias RabbitMQStream.Connection

  alias RabbitMQStream.Message.Command.{
    PeerProperties,
    SaslHandshake,
    SaslAuthenticate,
    Tune,
    Open,
    Heartbeat,
    Close,
    Create,
    Delete,
    StoreOffset,
    QueryOffset,
    DeclarePublisher,
    DeletePublisher,
    QueryMetadata,
    QueryPublisherSequence,
    Publish
  }

  alias RabbitMQStream.Message.Data.{
    TuneData,
    OpenData,
    PeerPropertiesData,
    SaslAuthenticateData,
    SaslHandshakeData,
    HeartbeatData,
    CloseData,
    CreateData,
    DeleteData,
    StoreOffsetData,
    QueryOffsetData,
    DeclarePublisherData,
    DeletePublisherData,
    QueryMetadataData,
    QueryPublisherSequenceData,
    PublishData
  }

  defstruct [
    :version,
    :correlation_id,
    :command,
    :data,
    :code
  ]

  def new!(%Connection{} = conn, %PeerProperties{}, _) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation_sequence,
      command: %PeerProperties{},
      data: %PeerPropertiesData{
        peer_properties: [
          {"product", "RabbitMQ Stream Client"},
          {"information", "Development"},
          {"version", "0.0.1"},
          {"platform", "Elixir"}
        ]
      }
    }
  end

  def new!(%Connection{} = conn, %SaslHandshake{}, _) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation_sequence,
      command: %SaslHandshake{},
      data: %SaslHandshakeData{
        mechanisms: [
          # "PLAIN"
        ]
      }
    }
  end

  def new!(%Connection{} = conn, %SaslAuthenticate{}, _) do
    cond do
      Enum.member?(conn.mechanisms, "PLAIN") ->
        %Request{
          version: conn.version,
          correlation_id: conn.correlation_sequence,
          command: %SaslAuthenticate{},
          data: %SaslAuthenticateData{
            mechanism: "PLAIN",
            sasl_opaque_data: [
              username: conn.username,
              password: conn.password
            ]
          }
        }

      true ->
        raise "Unsupported SASL mechanism: #{conn.mechanisms}"
    end
  end

  def new!(%Connection{} = conn, %Tune{}, _) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation_sequence,
      command: %Tune{},
      data: %TuneData{
        frame_max: conn.frame_max,
        heartbeat: conn.heartbeat
      }
    }
  end

  def new!(%Connection{} = conn, %Open{}, _) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation_sequence,
      command: %Open{},
      data: %OpenData{
        vhost: conn.vhost
      }
    }
  end

  def new!(%Connection{} = conn, %Heartbeat{}, _) do
    %Request{
      version: conn.version,
      command: %Heartbeat{},
      data: %HeartbeatData{}
    }
  end

  def new!(%Connection{} = conn, %Close{}, opts) do
    %Request{
      version: conn.version,
      command: %Close{},
      correlation_id: conn.correlation_sequence,
      data: %CloseData{
        code: opts[:code],
        reason: opts[:reason]
      }
    }
  end

  def new!(%Connection{} = conn, %Create{}, opts) do
    %Request{
      version: conn.version,
      command: %Create{},
      correlation_id: conn.correlation_sequence,
      data: %CreateData{
        stream_name: opts[:name],
        arguments: opts[:arguments]
      }
    }
  end

  def new!(%Connection{} = conn, %Delete{}, opts) do
    %Request{
      version: conn.version,
      command: %Delete{},
      correlation_id: conn.correlation_sequence,
      data: %DeleteData{
        stream_name: opts[:name]
      }
    }
  end

  def new!(%Connection{} = conn, %StoreOffset{}, opts) do
    %Request{
      version: conn.version,
      command: %StoreOffset{},
      correlation_id: conn.correlation_sequence,
      data: %StoreOffsetData{
        stream_name: opts[:stream_name],
        offset_reference: opts[:offset_reference],
        offset: opts[:offset]
      }
    }
  end

  def new!(%Connection{} = conn, %QueryOffset{}, opts) do
    %Request{
      version: conn.version,
      command: %QueryOffset{},
      correlation_id: conn.correlation_sequence,
      data: %QueryOffsetData{
        stream_name: opts[:stream_name],
        offset_reference: opts[:offset_reference]
      }
    }
  end

  def new!(%Connection{} = conn, %DeclarePublisher{}, opts) do
    %Request{
      version: conn.version,
      command: %DeclarePublisher{},
      correlation_id: conn.correlation_sequence,
      data: %DeclarePublisherData{
        id: conn.publisher_sequence,
        publisher_reference: opts[:publisher_reference],
        stream_name: opts[:stream_name]
      }
    }
  end

  def new!(%Connection{} = conn, %DeletePublisher{}, opts) do
    %Request{
      version: conn.version,
      command: %DeletePublisher{},
      correlation_id: conn.correlation_sequence,
      data: %DeletePublisherData{
        publisher_id: opts[:publisher_id]
      }
    }
  end

  def new!(%Connection{} = conn, %QueryMetadata{}, opts) do
    %Request{
      version: conn.version,
      command: %QueryMetadata{},
      correlation_id: conn.correlation_sequence,
      data: %QueryMetadataData{
        streams: opts[:streams]
      }
    }
  end

  def new!(%Connection{} = conn, %QueryPublisherSequence{}, opts) do
    %Request{
      version: conn.version,
      command: %QueryPublisherSequence{},
      correlation_id: conn.correlation_sequence,
      data: %QueryPublisherSequenceData{
        stream_name: opts[:stream_name],
        publisher_reference: opts[:publisher_reference]
      }
    }
  end

  def new!(%Connection{} = conn, %Publish{}, opts) do
    %Request{
      version: conn.version,
      command: %Publish{},
      data: %PublishData{
        publisher_id: opts[:publisher_id],
        published_messages: opts[:published_messages]
      }
    }
  end
end

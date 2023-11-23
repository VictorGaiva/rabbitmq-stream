defmodule RabbitMQStream.Message.Request do
  @moduledoc false
  require Logger
  alias __MODULE__

  alias RabbitMQStream.Connection

  alias RabbitMQStream.Message.Data.{
    TuneData,
    OpenData,
    PeerPropertiesData,
    SaslAuthenticateData,
    SaslHandshakeData,
    HeartbeatData,
    CloseData,
    CreateStreamData,
    DeleteStreamData,
    StoreOffsetData,
    QueryOffsetData,
    DeclarePublisherData,
    DeletePublisherData,
    QueryMetadataData,
    QueryPublisherSequenceData,
    PublishData,
    SubscribeRequestData,
    UnsubscribeRequestData
  }

  defstruct [
    :version,
    :correlation_id,
    :command,
    :data,
    :code
  ]

  def new!(%Connection{} = conn, :peer_properties, _) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation_sequence,
      command: :peer_properties,
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

  def new!(%Connection{} = conn, :sasl_handshake, _) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation_sequence,
      command: :sasl_handshake,
      data: %SaslHandshakeData{}
    }
  end

  def new!(%Connection{} = conn, :sasl_authenticate, _) do
    cond do
      Enum.member?(conn.mechanisms, "PLAIN") ->
        %Request{
          version: conn.version,
          correlation_id: conn.correlation_sequence,
          command: :sasl_authenticate,
          data: %SaslAuthenticateData{
            mechanism: "PLAIN",
            sasl_opaque_data: [
              username: conn.options[:username],
              password: conn.options[:password]
            ]
          }
        }

      true ->
        raise "Unsupported SASL mechanism: #{conn.mechanisms}"
    end
  end

  def new!(%Connection{} = conn, :tune, _) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation_sequence,
      command: :tune,
      data: %TuneData{
        frame_max: conn.options[:frame_max],
        heartbeat: conn.options[:heartbeat]
      }
    }
  end

  def new!(%Connection{} = conn, :open, _) do
    %Request{
      version: conn.version,
      correlation_id: conn.correlation_sequence,
      command: :open,
      data: %OpenData{
        vhost: conn.options[:vhost]
      }
    }
  end

  def new!(%Connection{} = conn, :heartbeat, _) do
    %Request{
      version: conn.version,
      command: :heartbeat,
      data: %HeartbeatData{}
    }
  end

  def new!(%Connection{} = conn, :close, opts) do
    %Request{
      version: conn.version,
      command: :close,
      correlation_id: conn.correlation_sequence,
      data: %CloseData{
        code: opts[:code],
        reason: opts[:reason]
      }
    }
  end

  def new!(%Connection{} = conn, :create_stream, opts) do
    %Request{
      version: conn.version,
      command: :create_stream,
      correlation_id: conn.correlation_sequence,
      data: %CreateStreamData{
        stream_name: opts[:name],
        arguments: opts[:arguments] || []
      }
    }
  end

  def new!(%Connection{} = conn, :delete_stream, opts) do
    %Request{
      version: conn.version,
      command: :delete_stream,
      correlation_id: conn.correlation_sequence,
      data: %DeleteStreamData{
        stream_name: opts[:name]
      }
    }
  end

  def new!(%Connection{} = conn, :store_offset, opts) do
    %Request{
      version: conn.version,
      command: :store_offset,
      correlation_id: conn.correlation_sequence,
      data: %StoreOffsetData{
        stream_name: opts[:stream_name],
        offset_reference: opts[:offset_reference],
        offset: opts[:offset]
      }
    }
  end

  def new!(%Connection{} = conn, :query_offset, opts) do
    %Request{
      version: conn.version,
      command: :query_offset,
      correlation_id: conn.correlation_sequence,
      data: %QueryOffsetData{
        stream_name: opts[:stream_name],
        offset_reference: opts[:offset_reference]
      }
    }
  end

  def new!(%Connection{} = conn, :declare_publisher, opts) do
    %Request{
      version: conn.version,
      command: :declare_publisher,
      correlation_id: conn.correlation_sequence,
      data: %DeclarePublisherData{
        id: conn.publisher_sequence,
        publisher_reference: opts[:publisher_reference],
        stream_name: opts[:stream_name]
      }
    }
  end

  def new!(%Connection{} = conn, :delete_publisher, opts) do
    %Request{
      version: conn.version,
      command: :delete_publisher,
      correlation_id: conn.correlation_sequence,
      data: %DeletePublisherData{
        publisher_id: opts[:publisher_id]
      }
    }
  end

  def new!(%Connection{} = conn, :query_metadata, opts) do
    %Request{
      version: conn.version,
      command: :query_metadata,
      correlation_id: conn.correlation_sequence,
      data: %QueryMetadataData{
        streams: opts[:streams]
      }
    }
  end

  def new!(%Connection{} = conn, :query_publisher_sequence, opts) do
    %Request{
      version: conn.version,
      command: :query_publisher_sequence,
      correlation_id: conn.correlation_sequence,
      data: %QueryPublisherSequenceData{
        stream_name: opts[:stream_name],
        publisher_reference: opts[:publisher_reference]
      }
    }
  end

  def new!(%Connection{} = conn, :publish, opts) do
    %Request{
      version: conn.version,
      command: :publish,
      data: %PublishData{
        publisher_id: opts[:publisher_id],
        published_messages: opts[:published_messages]
      }
    }
  end

  def new!(%Connection{} = conn, :subscribe, opts) do
    %Request{
      version: conn.version,
      command: :subscribe,
      correlation_id: conn.correlation_sequence,
      data: %SubscribeRequestData{
        credit: opts[:credit],
        offset: opts[:offset],
        properties: opts[:properties],
        stream_name: opts[:stream_name],
        subscription_id: opts[:subscription_id]
      }
    }
  end

  def new!(%Connection{} = conn, :unsubscribe, opts) do
    %Request{
      version: conn.version,
      command: :unsubscribe,
      correlation_id: conn.correlation_sequence,
      data: %UnsubscribeRequestData{
        subscription_id: opts[:subscription_id]
      }
    }
  end
end

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
    CreateStreamRequestData,
    DeleteStreamRequestData,
    StoreOffsetRequestData,
    QueryOffsetRequestData,
    DeclarePublisherRequestData,
    DeletePublisherRequestData,
    QueryMetadataRequestData,
    QueryPublisherSequenceData,
    PublishData,
    PublishDataV2,
    SubscribeRequestData,
    UnsubscribeRequestData,
    CreditRequestData,
    RouteRequestData,
    PartitionsQueryRequestData
  }

  defstruct [
    :version,
    :correlation_id,
    :command,
    :data,
    :code
  ]

  @version Mix.Project.config()[:version]

  def new!(%Connection{} = conn, :peer_properties, _) do
    %Request{
      version: 1,
      correlation_id: conn.correlation_sequence,
      command: :peer_properties,
      data: %PeerPropertiesData{
        peer_properties: [
          {"product", "RabbitMQ Stream Client"},
          {"information", "Development"},
          {"version", @version},
          {"platform", "Elixir"}
        ]
      }
    }
  end

  def new!(%Connection{} = conn, :sasl_handshake, _) do
    %Request{
      version: 1,
      correlation_id: conn.correlation_sequence,
      command: :sasl_handshake,
      data: %SaslHandshakeData{}
    }
  end

  def new!(%Connection{} = conn, :sasl_authenticate, _) do
    cond do
      Enum.member?(conn.mechanisms, "PLAIN") ->
        %Request{
          version: 1,
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
      version: 1,
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
      version: 1,
      correlation_id: conn.correlation_sequence,
      command: :open,
      data: %OpenData{
        vhost: conn.options[:vhost]
      }
    }
  end

  def new!(%Connection{}, :heartbeat, _) do
    %Request{
      version: 1,
      command: :heartbeat,
      data: %HeartbeatData{}
    }
  end

  def new!(%Connection{} = conn, :close, opts) do
    %Request{
      version: 1,
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
      version: 1,
      command: :create_stream,
      correlation_id: conn.correlation_sequence,
      data: %CreateStreamRequestData{
        stream_name: opts[:name],
        arguments: opts[:arguments] || []
      }
    }
  end

  def new!(%Connection{} = conn, :delete_stream, opts) do
    %Request{
      version: 1,
      command: :delete_stream,
      correlation_id: conn.correlation_sequence,
      data: %DeleteStreamRequestData{
        stream_name: opts[:name]
      }
    }
  end

  def new!(%Connection{} = conn, :store_offset, opts) do
    %Request{
      version: 1,
      command: :store_offset,
      correlation_id: conn.correlation_sequence,
      data: %StoreOffsetRequestData{
        stream_name: opts[:stream_name],
        offset_reference: opts[:offset_reference],
        offset: opts[:offset]
      }
    }
  end

  def new!(%Connection{} = conn, :query_offset, opts) do
    %Request{
      version: 1,
      command: :query_offset,
      correlation_id: conn.correlation_sequence,
      data: %QueryOffsetRequestData{
        stream_name: opts[:stream_name],
        offset_reference: opts[:offset_reference]
      }
    }
  end

  def new!(%Connection{} = conn, :declare_publisher, opts) do
    %Request{
      version: 1,
      command: :declare_publisher,
      correlation_id: conn.correlation_sequence,
      data: %DeclarePublisherRequestData{
        id: conn.publisher_sequence,
        publisher_reference: opts[:publisher_reference],
        stream_name: opts[:stream_name]
      }
    }
  end

  def new!(%Connection{} = conn, :delete_publisher, opts) do
    %Request{
      version: 1,
      command: :delete_publisher,
      correlation_id: conn.correlation_sequence,
      data: %DeletePublisherRequestData{
        publisher_id: opts[:publisher_id]
      }
    }
  end

  def new!(%Connection{} = conn, :query_metadata, opts) do
    %Request{
      version: 1,
      command: :query_metadata,
      correlation_id: conn.correlation_sequence,
      data: %QueryMetadataRequestData{
        streams: opts[:streams]
      }
    }
  end

  def new!(%Connection{} = conn, :query_publisher_sequence, opts) do
    %Request{
      version: 1,
      command: :query_publisher_sequence,
      correlation_id: conn.correlation_sequence,
      data: %QueryPublisherSequenceData{
        stream_name: opts[:stream_name],
        publisher_reference: opts[:publisher_reference]
      }
    }
  end

  def new!(%Connection{}, :publish, opts) do
    %Request{
      version: 1,
      command: :publish,
      data: %PublishData{
        publisher_id: opts[:publisher_id],
        published_messages: opts[:published_messages]
      }
    }
  end

  def new!(%Connection{}, :publish_v2, opts) do
    %Request{
      version: 2,
      command: :publish,
      data: %PublishDataV2{
        publisher_id: opts[:publisher_id],
        published_messages: opts[:published_messages],
        filter_value: opts[:filter_value] || "test123321123"
      }
    }
  end

  def new!(%Connection{} = conn, :subscribe, opts) do
    %Request{
      version: 1,
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
      version: 1,
      command: :unsubscribe,
      correlation_id: conn.correlation_sequence,
      data: %UnsubscribeRequestData{
        subscription_id: opts[:subscription_id]
      }
    }
  end

  def new!(%Connection{} = conn, :credit, opts) do
    %Request{
      version: 1,
      command: :credit,
      correlation_id: conn.correlation_sequence,
      data: %CreditRequestData{
        credit: opts[:credit],
        subscription_id: opts[:subscription_id]
      }
    }
  end

  def new!(%Connection{} = conn, :route, opts) do
    %Request{
      version: 1,
      command: :route,
      correlation_id: conn.correlation_sequence,
      data: %RouteRequestData{
        super_stream: opts[:super_stream],
        routing_key: opts[:routing_key]
      }
    }
  end

  def new!(%Connection{} = conn, :partitions, opts) do
    %Request{
      version: 1,
      command: :partitions,
      correlation_id: conn.correlation_sequence,
      data: %PartitionsQueryRequestData{
        super_stream: opts[:super_stream]
      }
    }
  end
end

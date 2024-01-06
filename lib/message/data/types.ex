defmodule RabbitMQStream.Message.Types do
  @moduledoc false

  defmodule TuneData do
    @moduledoc false

    defstruct [
      :frame_max,
      :heartbeat
    ]
  end

  defmodule PeerPropertiesData do
    @moduledoc false

    defstruct [
      :peer_properties
    ]
  end

  defmodule SaslHandshakeData do
    @moduledoc false

    defstruct [:mechanisms]
  end

  defmodule SaslAuthenticateData do
    @moduledoc false

    defstruct [
      :mechanism,
      :sasl_opaque_data
    ]
  end

  defmodule OpenData do
    @moduledoc false

    defstruct [
      :vhost,
      :connection_properties
    ]
  end

  defmodule HeartbeatData do
    @moduledoc false

    defstruct []
  end

  defmodule CloseData do
    @moduledoc false

    defstruct [
      :code,
      :reason
    ]
  end

  defmodule CreateStreamRequestData do
    @moduledoc false

    defstruct [
      :stream_name,
      :arguments
    ]
  end

  defmodule CreateStreamResponseData do
    @moduledoc false

    defstruct []
  end

  defmodule DeleteStreamRequestData do
    @moduledoc false

    defstruct [
      :stream_name
    ]
  end

  defmodule DeleteStreamResponseData do
    @moduledoc false

    defstruct [
      :stream_name
    ]
  end

  defmodule StoreOffsetRequestData do
    @moduledoc false

    defstruct [
      :offset_reference,
      :stream_name,
      :offset
    ]
  end

  defmodule StoreOffsetResponseData do
    @moduledoc false

    defstruct []
  end

  defmodule QueryOffsetRequestData do
    @moduledoc false

    defstruct [
      :offset_reference,
      :stream_name
    ]
  end

  defmodule QueryOffsetResponseData do
    @moduledoc false

    defstruct [
      :offset
    ]
  end

  defmodule QueryMetadataRequestData do
    @moduledoc false

    defstruct [
      :streams
    ]
  end

  defmodule QueryMetadataResponseData do
    @moduledoc false

    defstruct [
      :streams,
      :brokers
    ]
  end

  defmodule MetadataUpdateData do
    @moduledoc false

    defstruct [
      :stream_name,
      :code
    ]
  end

  defmodule DeclarePublisherRequestData do
    @moduledoc false

    defstruct [
      :id,
      :publisher_reference,
      :stream_name
    ]
  end

  defmodule DeclarePublisherResponseData do
    @moduledoc false

    defstruct []
  end

  defmodule DeletePublisherRequestData do
    @moduledoc false

    defstruct [:publisher_id]
  end

  defmodule DeletePublisherResponseData do
    @moduledoc false

    defstruct []
  end

  defmodule BrokerData do
    @moduledoc false

    defstruct [
      :reference,
      :host,
      :port
    ]
  end

  defmodule StreamData do
    @moduledoc false

    defstruct [
      :code,
      :name,
      :leader,
      :replicas
    ]
  end

  defmodule QueryPublisherSequenceData do
    @moduledoc false

    defstruct [
      :publisher_reference,
      :stream_name,
      :sequence
    ]
  end

  defmodule PublishData do
    @moduledoc false

    defstruct [
      :publisher_id,
      :messages
    ]
  end

  defmodule PublishV2Data do
    @moduledoc false

    defstruct [
      :publisher_id,
      :messages,
      :filter_value
    ]
  end

  defmodule PublishErrorData do
    @moduledoc false

    defmodule Error do
      @moduledoc false

      defstruct [
        :publishing_id,
        :code
      ]
    end

    defstruct [
      :publisher_id,
      :errors
    ]
  end

  defmodule PublishConfirmData do
    @moduledoc false

    defstruct [
      :publisher_id,
      :publishing_ids
    ]
  end

  defmodule SubscribeRequestData do
    @moduledoc false
    # Supported properties:

    # * `single-active-consumer`: set to `true` to enable https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-single-active-consumer-for-streams/[single active consumer] for this subscription.
    # * `super-stream`: set to the name of the super stream the subscribed is a partition of.
    # * `filter.` (e.g. `filter.0`, `filter.1`, etc): prefix to use to define filter values for the subscription.
    # * `match-unfiltered`: whether to return messages without any filter value or not.

    @type t :: %{
            subscription_id: non_neg_integer(),
            stream_name: String.t(),
            offset: RabbitMQStream.Connection.offset(),
            credit: non_neg_integer(),
            properties: %{String.t() => String.t()}
          }

    defstruct [
      :subscription_id,
      :stream_name,
      :offset,
      :credit,
      :properties
    ]
  end

  defmodule UnsubscribeRequestData do
    @moduledoc false

    @type t :: %{
            subscription_id: non_neg_integer()
          }

    defstruct [
      :subscription_id
    ]
  end

  defmodule CreditRequestData do
    @moduledoc false

    @type t :: %{
            subscription_id: non_neg_integer(),
            credit: non_neg_integer()
          }

    defstruct [
      :subscription_id,
      :credit
    ]
  end

  defmodule SubscribeResponseData do
    @moduledoc false
    @type t :: %{}
    defstruct []
  end

  defmodule UnsubscribeResponseData do
    @moduledoc false
    @type t :: %{}
    defstruct []
  end

  defmodule CreditResponseData do
    @moduledoc false
    @type t :: %{}
    defstruct []
  end

  defmodule RouteRequestData do
    @moduledoc false
    @enforce_keys [:routing_key, :super_stream]
    @type t :: %{
            routing_key: String.t(),
            super_stream: String.t()
          }
    defstruct [
      :routing_key,
      :super_stream
    ]
  end

  defmodule RouteResponseData do
    @moduledoc false
    @enforce_keys [:stream]
    @type t :: %{stream: String.t()}
    defstruct [:stream]
  end

  defmodule PartitionsQueryRequestData do
    @moduledoc false
    @enforce_keys [:super_stream]
    @type t :: %{super_stream: String.t()}
    defstruct [:super_stream]
  end

  defmodule PartitionsQueryResponseData do
    @moduledoc false
    @enforce_keys [:stream]
    @type t :: %{stream: String.t()}
    defstruct [:stream]
  end

  defmodule DeliverData do
    @moduledoc false
    @enforce_keys [:subscription_id, :osiris_chunk]
    @type t :: %{
            committed_offset: non_neg_integer() | nil,
            subscription_id: non_neg_integer(),
            osiris_chunk: RabbitMQStream.OsirisChunk.t()
          }
    defstruct [
      :committed_offset,
      :subscription_id,
      :osiris_chunk
    ]
  end

  defmodule ExchangeCommandVersionsData do
    @moduledoc false
    @enforce_keys [:commands]
    @type t :: %{
            commands: [Command.t()]
          }
    defstruct [:commands]

    defmodule Command do
      @moduledoc false
      @enforce_keys [:key, :min_version, :max_version]
      @type t :: %{
              key: RabbitMQStream.Message.Helpers.command(),
              min_version: non_neg_integer(),
              max_version: non_neg_integer()
            }
      defstruct [:key, :min_version, :max_version]
    end

    def new!(_opts \\ []) do
      %__MODULE__{
        commands: [
          %Command{key: :publish, min_version: 1, max_version: 2},
          %Command{key: :deliver, min_version: 1, max_version: 2},
          %Command{key: :declare_publisher, min_version: 1, max_version: 1},
          %Command{key: :publish_confirm, min_version: 1, max_version: 1},
          %Command{key: :publish_error, min_version: 1, max_version: 1},
          %Command{key: :query_publisher_sequence, min_version: 1, max_version: 1},
          %Command{key: :delete_publisher, min_version: 1, max_version: 1},
          %Command{key: :subscribe, min_version: 1, max_version: 1},
          %Command{key: :credit, min_version: 1, max_version: 1},
          %Command{key: :store_offset, min_version: 1, max_version: 1},
          %Command{key: :query_offset, min_version: 1, max_version: 1},
          %Command{key: :unsubscribe, min_version: 1, max_version: 1},
          %Command{key: :create_stream, min_version: 1, max_version: 1},
          %Command{key: :delete_stream, min_version: 1, max_version: 1},
          %Command{key: :query_metadata, min_version: 1, max_version: 1},
          %Command{key: :metadata_update, min_version: 1, max_version: 1},
          %Command{key: :peer_properties, min_version: 1, max_version: 1},
          %Command{key: :sasl_handshake, min_version: 1, max_version: 1},
          %Command{key: :sasl_authenticate, min_version: 1, max_version: 1},
          %Command{key: :tune, min_version: 1, max_version: 1},
          %Command{key: :open, min_version: 1, max_version: 1},
          %Command{key: :close, min_version: 1, max_version: 1},
          %Command{key: :heartbeat, min_version: 1, max_version: 1},
          %Command{key: :route, min_version: 1, max_version: 1},
          %Command{key: :partitions, min_version: 1, max_version: 1},
          %Command{key: :consumer_update, min_version: 1, max_version: 1},
          %Command{key: :exchange_command_versions, min_version: 1, max_version: 1},
          %Command{key: :stream_stats, min_version: 1, max_version: 1},
          %Command{key: :create_super_stream, min_version: 1, max_version: 1},
          %Command{key: :delete_super_stream, min_version: 1, max_version: 1}
        ]
      }
    end

    def encode!(%__MODULE__{commands: commands}) do
      RabbitMQStream.Message.Helpers.encode_array(
        for command <- commands do
          <<
            RabbitMQStream.Message.Helpers.encode_command(command.key)::unsigned-integer-size(16),
            command.min_version::unsigned-integer-size(16),
            command.max_version::unsigned-integer-size(16)
          >>
        end
      )
    end

    def decode!(buffer) do
      {"", commands} =
        RabbitMQStream.Message.Helpers.decode_array(buffer, fn buffer, acc ->
          <<
            key::unsigned-integer-size(16),
            min_version::unsigned-integer-size(16),
            max_version::unsigned-integer-size(16),
            rest::binary
          >> = buffer

          value = %Command{
            key: RabbitMQStream.Message.Helpers.decode_command(key),
            min_version: min_version,
            max_version: max_version
          }

          {rest, [value | acc]}
        end)

      %__MODULE__{commands: commands}
    end
  end
end

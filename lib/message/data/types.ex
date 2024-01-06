defmodule RabbitMQStream.Message.Data.Types do
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
end

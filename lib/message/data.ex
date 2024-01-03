defmodule RabbitMQStream.Message.Data do
  @moduledoc false
  alias RabbitMQStream.Message.{Response, Request, Frame}

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
      :published_messages
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

  def decode_data(%{command: :heartbeat}, ""), do: %HeartbeatData{}
  def decode_data(%Response{command: :close}, ""), do: %CloseData{}

  def decode_data(%Response{command: :create_stream}, ""), do: %CreateStreamResponseData{}
  def decode_data(%Response{command: :delete_stream}, ""), do: %DeleteStreamResponseData{}
  def decode_data(%Response{command: :declare_publisher}, ""), do: %DeclarePublisherResponseData{}
  def decode_data(%Response{command: :delete_publisher}, ""), do: %DeletePublisherResponseData{}
  def decode_data(%Response{command: :subscribe}, ""), do: %SubscribeResponseData{}
  def decode_data(%Response{command: :unsubscribe}, ""), do: %UnsubscribeResponseData{}
  def decode_data(%Response{command: :credit}, ""), do: %CreditResponseData{}
  def decode_data(%Response{command: :store_offset}, ""), do: %StoreOffsetResponseData{}

  def decode_data(%Request{command: :publish_confirm}, buffer) do
    <<publisher_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", publishing_ids} =
      decode_array(buffer, fn buffer, acc ->
        <<publishing_id::unsigned-integer-size(64), buffer::binary>> = buffer
        {buffer, [publishing_id] ++ acc}
      end)

    %PublishConfirmData{publisher_id: publisher_id, publishing_ids: publishing_ids}
  end

  def decode_data(%Response{command: :publish_error}, buffer) do
    <<publisher_id::unsigned-integer-size(8), buffer::binary>> = buffer

    {"", errors} =
      decode_array(buffer, fn buffer, acc ->
        <<
          publishing_id::unsigned-integer-size(64),
          code::unsigned-integer-size(16),
          buffer::binary
        >> = buffer

        entry = %PublishErrorData.Error{
          code: Frame.response_code_to_atom(code),
          publishing_id: publishing_id
        }

        {buffer, [entry] ++ acc}
      end)

    %PublishErrorData{publisher_id: publisher_id, errors: errors}
  end

  def decode_data(%Request{version: 1, command: :deliver}, buffer) do
    <<subscription_id::unsigned-integer-size(8), rest::binary>> = buffer

    osiris_chunk = RabbitMQStream.OsirisChunk.decode!(rest)

    %DeliverData{subscription_id: subscription_id, osiris_chunk: osiris_chunk}
  end

  def decode_data(%Request{version: 2, command: :deliver}, buffer) do
    <<subscription_id::unsigned-integer-size(8), committed_offset::unsigned-integer-size(64), rest::binary>> = buffer

    osiris_chunk = RabbitMQStream.OsirisChunk.decode!(rest)

    %DeliverData{
      subscription_id: subscription_id,
      committed_offset: committed_offset,
      osiris_chunk: osiris_chunk
    }
  end

  def decode_data(%{command: :query_metadata}, buffer) do
    {buffer, brokers} =
      decode_array(buffer, fn buffer, acc ->
        <<reference::unsigned-integer-size(16), buffer::binary>> = buffer

        <<size::integer-size(16), host::binary-size(size), buffer::binary>> = buffer

        <<port::unsigned-integer-size(32), buffer::binary>> = buffer

        data = %BrokerData{
          reference: reference,
          host: host,
          port: port
        }

        {buffer, [data] ++ acc}
      end)

    {"", streams} =
      decode_array(buffer, fn buffer, acc ->
        <<
          size::integer-size(16),
          name::binary-size(size),
          code::unsigned-integer-size(16),
          leader::unsigned-integer-size(16),
          buffer::binary
        >> = buffer

        {buffer, replicas} =
          decode_array(buffer, fn buffer, acc ->
            <<replica::unsigned-integer-size(16), buffer::binary>> = buffer

            {buffer, [replica] ++ acc}
          end)

        data = %StreamData{
          code: code,
          name: name,
          leader: leader,
          replicas: replicas
        }

        {buffer, [data] ++ acc}
      end)

    %QueryMetadataResponseData{brokers: brokers, streams: streams}
  end

  def decode_data(%Request{command: :close}, <<code::unsigned-integer-size(16), buffer::binary>>) do
    {"", reason} = fetch_string(buffer)

    %CloseData{code: code, reason: reason}
  end

  def decode_data(%{command: :metadata_update}, <<code::unsigned-integer-size(16), buffer::binary>>) do
    {"", stream_name} = fetch_string(buffer)

    %MetadataUpdateData{stream_name: stream_name, code: code}
  end

  def decode_data(%{command: :query_offset}, <<offset::unsigned-integer-size(64)>>) do
    %QueryOffsetResponseData{offset: offset}
  end

  def decode_data(%Response{command: :query_publisher_sequence}, <<sequence::unsigned-integer-size(64)>>) do
    %QueryPublisherSequenceData{sequence: sequence}
  end

  def decode_data(%{command: :peer_properties}, buffer) do
    {"", peer_properties} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, key} = fetch_string(buffer)
        {buffer, value} = fetch_string(buffer)

        {buffer, [{key, value} | acc]}
      end)

    %PeerPropertiesData{peer_properties: peer_properties}
  end

  def decode_data(%{command: :sasl_handshake}, buffer) do
    {"", mechanisms} =
      decode_array(buffer, fn buffer, acc ->
        {buffer, value} = fetch_string(buffer)
        {buffer, [value | acc]}
      end)

    %SaslHandshakeData{mechanisms: mechanisms}
  end

  def decode_data(%{command: :sasl_authenticate}, buffer) do
    %SaslAuthenticateData{sasl_opaque_data: buffer}
  end

  def decode_data(%{command: :tune}, <<frame_max::unsigned-integer-size(32), heartbeat::unsigned-integer-size(32)>>) do
    %TuneData{frame_max: frame_max, heartbeat: heartbeat}
  end

  def decode_data(%{command: :open}, buffer) do
    connection_properties =
      if buffer != "" do
        {"", connection_properties} =
          decode_array(buffer, fn buffer, acc ->
            {buffer, key} = fetch_string(buffer)
            {buffer, value} = fetch_string(buffer)

            {buffer, [{key, value} | acc]}
          end)

        connection_properties
      else
        []
      end

    %OpenData{connection_properties: connection_properties}
  end

  def decode_data(%Response{command: :route}, buffer) do
    {"", stream} = fetch_string(buffer)

    %RouteResponseData{stream: stream}
  end

  def decode_data(%Response{command: :partitions}, buffer) do
    {"", stream} = fetch_string(buffer)

    %RouteResponseData{stream: stream}
  end

  def fetch_string(<<size::integer-size(16), text::binary-size(size), rest::binary>>) do
    {rest, to_string(text)}
  end

  def decode_array("", _) do
    {"", []}
  end

  def decode_array(<<0::integer-size(32), buffer::binary>>, _) do
    {buffer, []}
  end

  def decode_array(<<size::integer-size(32), buffer::binary>>, foo) do
    Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
      foo.(buffer, acc)
    end)
  end

  def decode_array(<<0::integer-size(32), buffer::binary>>, _) do
    {buffer, []}
  end

  def decode_array(<<size::integer-size(32), buffer::binary>>, foo) do
    Enum.reduce(0..(size - 1), {buffer, []}, fn _, {buffer, acc} ->
      foo.(buffer, acc)
    end)
  end
end

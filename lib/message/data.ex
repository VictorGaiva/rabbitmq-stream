defmodule RabbitMQStream.Message.Data do
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

    defstruct [
      :mechanisms
    ]
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

  defmodule CreateData do
    @moduledoc false

    defstruct [
      :stream_name,
      :arguments
    ]
  end

  defmodule DeleteData do
    @moduledoc false

    defstruct [
      :stream_name
    ]
  end

  defmodule StoreOffsetData do
    @moduledoc false

    defstruct [
      :offset_reference,
      :stream_name,
      :offset
    ]
  end

  defmodule QueryOffsetData do
    @moduledoc false

    defstruct [
      :offset_reference,
      :stream_name,
      :offset
    ]
  end

  defmodule QueryMetadataData do
    @moduledoc false

    defstruct [
      :brokers,
      :streams
    ]
  end

  defmodule MetadataUpdateData do
    @moduledoc false

    defstruct [
      :stream_name
    ]
  end

  defmodule DeclarePublisherData do
    @moduledoc false

    defstruct [
      :id,
      :publisher_reference,
      :stream_name
    ]
  end

  defmodule DeletePublisherData do
    @moduledoc false

    defstruct [
      :publisher_id
    ]
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

  defmodule DeliverData do
    @moduledoc false
    @type t :: %{
            subscription_id: non_neg_integer(),
            osiris_chunk: Helpers.OsirisChunk.t() | nil
          }
    defstruct [
      :subscription_id,
      :osiris_chunk
    ]
  end
end

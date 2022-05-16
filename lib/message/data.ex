defmodule RabbitStream.Message.Data do
  defmodule TuneData do
    defstruct [
      :frame_max,
      :heartbeat
    ]
  end

  defmodule PeerPropertiesData do
    defstruct [
      :peer_properties
    ]
  end

  defmodule SaslHandshakeData do
    defstruct [
      :mechanisms
    ]
  end

  defmodule SaslAuthenticateData do
    defstruct [
      :mechanism,
      :sasl_opaque_data
    ]
  end

  defmodule OpenData do
    defstruct [
      :vhost,
      :connection_properties
    ]
  end

  defmodule HeartbeatData do
    defstruct []
  end

  defmodule CloseData do
    defstruct [
      :code,
      :reason
    ]
  end

  defmodule CreateData do
    defstruct [
      :stream_name,
      :arguments
    ]
  end

  defmodule DeleteData do
    defstruct [
      :stream_name
    ]
  end

  defmodule StoreOffsetData do
    defstruct [
      :reference,
      :stream_name,
      :offset
    ]
  end

  defmodule QueryOffsetData do
    defstruct [
      :reference,
      :stream_name,
      :offset
    ]
  end

  defmodule MetadataUpdateData do
    defstruct [
      :stream_name
    ]
  end
end

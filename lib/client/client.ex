defmodule RabbitMQStream.Client do
  @moduledoc """
  Client for connection to a RabbitMQ Stream Cluster.

  This module provides the API for setting up and managing a connection to multiple RabbitMQ Stream
  nodes in a cluster. It implements RabbitMQStream.Connection.Behavior, but with added functionality
  related to managing multiple connections.
  """

  def start_link(opts \\ []) when is_list(opts) do
    opts =
      Application.get_env(:rabbitmq_stream, :defaults, [])
      |> Keyword.get(:client, [])
      |> Keyword.merge(opts)

    GenServer.start_link(RabbitMQStream.Client.Lifecycle, opts, name: opts[:name])
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  defdelegate subscribe(server, stream_name, pid, offset, credit, properties \\ []), to: RabbitMQStream.Connection

  defmodule ConnectionState do
    @moduledoc false
    defstruct [:pid, state: :initial, streams: []]

    @type t :: %__MODULE__{
            state: :initial | :opening | :open | :closed,
            pid: pid(),
            streams: [binary()]
          }
  end

  defmodule BrokerState do
    @moduledoc false
    defstruct [:data, connections: []]

    @type t :: %__MODULE__{
            data: RabbitMQStream.Message.Types.QueryMetadataResponseData.BrokerData.t(),
            # First let's assume there is up to one connection per broker
            connections: %{reference() => ConnectionState.t()}
          }
  end

  @type client_option ::
          {:auto_discovery, boolean()}
          | {:connection_opts, RabbitMQStream.Connection.connection_options()}

  defstruct status: :setup, opts: [], control: nil, brokers: %{}, streams: %{}, connections: %{}

  @type t :: %__MODULE__{
          control: pid() | nil,
          status: :open | :setup | :closed,
          # Maps each broker reference to its state
          brokers: %{non_neg_integer() => __MODULE__.BrokerState.t()},
          # Maps a stream to the broker reference
          streams: %{binary() => non_neg_integer()},
          # Maps each connection ref to the related broker reference
          connections: %{reference() => non_neg_integer()},
          opts: [RabbitMQStream.Connection.connection_options() | client_option()]
        }
end

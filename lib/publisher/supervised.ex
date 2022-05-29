defmodule RabbitMQStream.SupervisedPublisher do
  @moduledoc """
  Provides an interface for declaring a standalone Publisher, that supervises its own `RabbitMQStream.Connection`
  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use Supervisor
      @connection Keyword.get(opts, :connection) || raise("Connection is required")
      @stream_name Keyword.get(opts, :stream_name) || raise("Stream Name is required")

      def start_link(init_arg) do
        Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
      end

      @impl true
      def init(_) do
        children = [
          {RabbitMQStream.Connection, @connection ++ [name: __MODULE__.Connection]},
          {RabbitMQStream.Publisher,
           [
             connection: __MODULE__.Connection,
             reference_name: __MODULE__.Publisher |> Atom.to_string(),
             stream_name: @stream_name,
             name: __MODULE__.Publisher
           ]}
        ]

        Supervisor.init(children, strategy: :one_for_all)
      end

      def publish(message, opts \\ nil) do
        RabbitMQStream.Publisher.publish(__MODULE__.Publisher, message, opts)
      end

      def get_publisher_state() do
        RabbitMQStream.Publisher.get_state(__MODULE__.Publisher)
      end
    end
  end
end

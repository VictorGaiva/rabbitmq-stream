defmodule RabbitStream.Publisher.Supervisor do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use Supervisor

      def start_link(init_arg) do
        Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
      end

      @impl true
      def init(_) do
        children =
          if not is_atom(opts[:connection]) do
            [
              {RabbitStream.Connection, opts[:connection], name: __MODULE__.Connection},
              {RabbitStream.Publisher,
               opts ++ [connection: __MODULE__.Connection, reference_name: __MODULE__.Publisher],
               name: __MODULE__.Publisher}
            ]
          else
            [
              {RabbitStream.Publisher, opts ++ [reference_name: __MODULE__.Publisher], name: __MODULE__.Publisher}
            ]
          end

        Supervisor.init(children, strategy: :one_for_all)
      end

      def publish(message, opts \\ []) do
        GenServer.call(__MODULE__.Publisher, {:publish, message, opts})
      end
    end
  end
end

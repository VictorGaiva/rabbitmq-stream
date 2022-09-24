defmodule RabbitMQStream.Connection.Lifecycle do
  @moduledoc false

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      require Logger
      use GenServer

      alias RabbitMQStream.Connection.Handler
      alias RabbitMQStream.Connection

      @impl true
      def handle_call({:get_state}, _from, %Connection{} = conn) do
        {:reply, conn, conn}
      end

      def handle_call({:connect}, from, %Connection{state: :closed} = conn) do
        Logger.info("Connecting to server: #{conn.options[:host]}:#{conn.options[:port]}")

        with {:ok, socket} <-
               :gen_tcp.connect(String.to_charlist(conn.options[:host]), conn.options[:port], [:binary, active: true]),
             :ok <- :gen_tcp.controlling_process(socket, self()) do
          Logger.debug("Connection stablished. Initiating properties exchange.")

          conn =
            %{conn | socket: socket, state: :connecting, connect_requests: [from]}
            |> Handler.send_request(:peer_properties)

          {:noreply, conn}
        else
          err ->
            Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}")
            {:reply, {:error, err}, conn}
        end
      end

      def handle_call({:connect}, _from, %Connection{state: :open} = conn) do
        {:reply, :ok, conn}
      end

      def handle_call({:connect}, from, %Connection{} = conn) do
        conn = %{conn | connect_requests: conn.connect_requests ++ [from]}
        {:noreply, conn}
      end

      def handle_call(_, _from, %Connection{state: state} = conn) when state != :open do
        {:reply, {:error, state}, conn}
      end

      def handle_call({:close, reason, code}, from, %Connection{} = conn) do
        Logger.info("Connection close requested by client: #{reason} #{code}")

        conn =
          %{conn | state: :closing}
          |> Handler.push_request_tracker(:close, from)
          |> Handler.send_request(:close, reason: reason, code: code)

        {:noreply, conn}
      end

      def handle_call({:subscribe, opts}, from, %Connection{} = conn) do
        subscription_id = conn.subscriber_sequence

        conn =
          conn
          |> Handler.push_request_tracker(:subscribe, from, {subscription_id, opts[:pid]})
          |> Handler.send_request(:subscribe, opts ++ [subscriber_sum: 1, subscription_id: subscription_id])

        {:noreply, conn}
      end

      def handle_call({:unsubscribe, opts}, from, %Connection{} = conn) do
        conn =
          conn
          |> Handler.push_request_tracker(:unsubscribe, from, opts[:subscription_id])
          |> Handler.send_request(:unsubscribe, opts)

        {:noreply, conn}
      end

      def handle_call({command, opts}, from, %Connection{} = conn)
          when command in [
                 :query_offset,
                 :delete_publisher,
                 :query_metadata,
                 :query_publisher_sequence,
                 :delete_stream,
                 :create_stream
               ] do
        conn =
          conn
          |> Handler.push_request_tracker(command, from)
          |> Handler.send_request(command, opts)

        {:noreply, conn}
      end

      def handle_call({:store_offset, opts}, _from, %Connection{} = conn) do
        conn =
          conn
          |> Handler.send_request(:store_offset, opts)

        {:reply, :ok, conn}
      end

      def handle_call({:declare_publisher, opts}, from, %Connection{} = conn) do
        conn =
          conn
          |> Handler.push_request_tracker(:declare_publisher, from, conn.publisher_sequence)
          |> Handler.send_request(:declare_publisher, opts ++ [publisher_sum: 1])

        {:noreply, conn}
      end

      @impl true
      def handle_cast({:publish, opts}, %Connection{} = conn) do
        {_wait, opts} = Keyword.pop(opts, :wait, false)

        # conn =
        #   if wait do
        #     publishing_ids = Enum.map(opts[:published_messages], fn {id, _} -> id end)
        #     publish_tracker = PublishingTracker.push(conn.publish_tracker, opts[:publisher_id], publishing_ids, from)
        #     %{conn | publish_tracker: publish_tracker}
        #   else
        #     conn
        #   end

        conn =
          conn
          |> Handler.send_request(:publish, opts ++ [correlation_sum: 0])

        {:noreply, conn}
      end

      @impl true
      def handle_info({:tcp, _socket, data}, conn) do
        {commands, buffer} =
          data
          |> :rabbit_stream_core.incoming_data(conn.buffer)
          |> :rabbit_stream_core.all_commands()

        conn = %{conn | buffer: buffer}

        conn = Enum.reduce(commands, conn, &Handler.handle_message/2)

        cond do
          conn.state == :closed ->
            {:noreply, conn, :hibernate}

          true ->
            {:noreply, conn}
        end
      end

      def handle_info({:heartbeat}, conn) do
        conn = Handler.send_request(conn, :heartbeat, correlation_sum: 0)

        Process.send_after(self(), {:heartbeat}, conn.options[:heartbeat] * 1000)

        {:noreply, conn}
      end

      def handle_info({:tcp_closed, _socket}, conn) do
        if conn.state == :connecting do
          Logger.warn(
            "The connection was closed by the host, after the socket was already open, while running the authentication sequence. This could be caused by the server not having Stream Plugin active"
          )
        end

        conn = %{conn | socket: nil, state: :closed} |> Handler.handle_closed(:tcp_closed)

        {:noreply, conn, :hibernate}
      end

      def handle_info({:tcp_error, _socket, reason}, conn) do
        conn = %{conn | socket: nil, state: :closed} |> Handler.handle_closed(reason)

        {:noreply, conn}
      end

      @impl true
      def handle_continue({:connect}, %Connection{state: :closed} = conn) do
        Logger.info("Connecting to server: #{conn.options[:host]}:#{conn.options[:port]}")

        with {:ok, socket} <-
               :gen_tcp.connect(String.to_charlist(conn.options[:host]), conn.options[:port], [:binary, active: true]),
             :ok <- :gen_tcp.controlling_process(socket, self()) do
          Logger.debug("Connection stablished. Initiating properties exchange.")

          conn =
            %{conn | socket: socket, state: :connecting}
            |> Handler.send_request(:peer_properties)

          {:noreply, conn}
        else
          _ ->
            Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}")
            {:noreply, conn}
        end
      end
    end
  end
end

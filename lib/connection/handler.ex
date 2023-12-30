defmodule RabbitMQStream.Connection.Handler do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      require Logger
      alias RabbitMQStream.{Message, Connection}
      alias RabbitMQStream.Message.{Encoder, Request, Response}
      alias RabbitMQStream.Connection.Helpers

      defp handle_message(%Request{command: :close} = request, conn) do
        Logger.debug("Connection close requested by server: #{request.data.code} #{request.data.reason}")
        Logger.debug("Connection closed")

        %{conn | state: :closing}
        |> send_response(:close, correlation_id: request.correlation_id, code: :ok)
        |> handle_closed(request.data.reason)
      end

      defp handle_message(%Request{command: :tune} = request, conn) do
        Logger.debug("Tunning complete. Starting heartbeat timer.")

        Process.send_after(self(), {:heartbeat}, conn.options[:heartbeat] * 1000)

        options = Keyword.merge(conn.options, frame_max: request.data.frame_max, heartbeat: request.data.heartbeat)

        %{conn | options: options}
        |> send_response(:tune, correlation_id: 0)
        |> Map.put(:state, :opening)
        |> send_request(:open)
      end

      defp handle_message(%Request{command: :heartbeat}, conn) do
        conn
      end

      defp handle_message(%Request{command: :metadata_update} = request, conn) do
        conn
        |> send_request(:query_metadata, streams: [request.data.stream_name])
      end

      defp handle_message(%Request{command: :deliver} = response, conn) do
        pid = Map.get(conn.subscriptions, response.data.subscription_id)

        if pid != nil do
          send(pid, {:chunk, response.data.osiris_chunk})
        end

        conn
      end

      defp handle_message(%Request{command: command}, conn)
           when command in [:publish_confirm, :publish_error] do
        conn
      end

      defp handle_message(%Response{command: :close} = response, conn) do
        Logger.debug("Connection closed: #{conn.options[:host]}:#{conn.options[:port]}")

        {{pid, _data}, conn} = Helpers.pop_request_tracker(conn, :close, response.correlation_id)

        conn = %{conn | state: :closed, socket: nil}

        GenServer.reply(pid, :ok)

        conn
      end

      defp handle_message(%Response{code: code}, conn)
           when code in [
                  :sasl_mechanism_not_supported,
                  :authentication_failure,
                  :sasl_error,
                  :sasl_challenge,
                  :sasl_authentication_failure_loopback,
                  :virtual_host_access_failure
                ] do
        Logger.error("Failed to connect to #{conn.options[:host]}:#{conn.options[:port]}. Reason: #{code}")

        for request <- conn.connect_requests do
          GenServer.reply(request, {:error, code})
        end

        %{conn | state: :closed, socket: nil, connect_requests: []}
      end

      defp handle_message(%Response{command: :credit, code: code}, conn)
           when code not in [:ok, nil] do
        Logger.error("Failed to credit subscription. Reason: #{code}")

        conn
      end

      defp handle_message(%Response{command: command, code: code} = response, conn)
           when command in [
                  :create_stream,
                  :delete_stream,
                  :query_offset,
                  :declare_publisher,
                  :delete_publisher,
                  :subscribe,
                  :unsubscribe
                ] and
                  code not in [:ok, nil] do
        {{pid, _data}, conn} = Helpers.pop_request_tracker(conn, command, response.correlation_id)

        if pid != nil do
          GenServer.reply(pid, {:error, code})
        end

        conn
      end

      defp handle_message(_, %Connection{state: :closed} = conn) do
        Logger.error("Message received on a closed connection")

        conn
      end

      defp handle_message(%Response{command: :peer_properties} = response, conn) do
        Logger.debug("Exchange successful.")
        Logger.debug("Initiating SASL handshake.")

        %{conn | peer_properties: response.data.peer_properties}
        |> send_request(:sasl_handshake)
      end

      defp handle_message(%Response{command: :sasl_handshake} = response, conn) do
        Logger.debug("SASL handshake successful. Initiating authentication.")

        %{conn | mechanisms: response.data.mechanisms}
        |> send_request(:sasl_authenticate)
      end

      defp handle_message(%Response{command: :sasl_authenticate, data: %{sasl_opaque_data: ""}}, conn) do
        Logger.debug("Authentication successful. Initiating connection tuning.")

        conn
      end

      defp handle_message(%Response{command: :sasl_authenticate}, conn) do
        Logger.debug("Authentication successful. Skipping connection tuning.")
        Logger.debug("Opening connection to vhost: \"#{conn.options[:vhost]}\"")

        conn
        |> send_request(:open)
        |> Map.put(:state, :opening)
      end

      defp handle_message(%Response{command: :tune} = response, conn) do
        Logger.debug("Tunning data received. Starting heartbeat timer.")
        Logger.debug("Opening connection to vhost: \"#{conn.options[:vhost]}\"")

        options = Keyword.merge(conn.options, frame_max: response.data.frame_max, heartbeat: response.data.heartbeat)

        %{conn | options: options}
        |> Map.put(:state, :opening)
        |> send_request(:open)
      end

      defp handle_message(%Response{command: :open} = response, conn) do
        Logger.debug("Successfully opened connection with vhost: \"#{conn.options[:vhost]}\"")

        for request <- conn.connect_requests do
          GenServer.reply(request, :ok)
        end

        send(self(), :flush_request_buffer)

        %{conn | state: :open, connect_requests: [], connection_properties: response.data.connection_properties}
      end

      defp handle_message({:response, correlation_id, {:metadata, brokers, streams}}, conn) do
        {{pid, _data}, conn} = Helpers.pop_request_tracker(conn, :query_metadata, correlation_id)

        brokers = Map.new(brokers)

        if pid != nil do
          GenServer.reply(pid, {:ok, %{brokers: brokers, streams: streams}})
        end

        %{conn | brokers: Map.merge(conn.brokers, brokers), streams: Map.merge(conn.streams, streams)}
      end

      defp handle_message(%Response{command: :query_metadata} = response, conn) do
        {{pid, _data}, conn} = Helpers.pop_request_tracker(conn, :query_metadata, response.correlation_id)

        if pid != nil do
          GenServer.reply(pid, {:ok, response.data})
        end

        %{conn | streams: response.data.streams, brokers: response.data.brokers}
      end

      defp handle_message(%Response{command: :query_offset} = response, conn) do
        {{pid, _data}, conn} = Helpers.pop_request_tracker(conn, :query_offset, response.correlation_id)

        if pid != nil do
          GenServer.reply(pid, {:ok, response.data.offset})
        end

        conn
      end

      defp handle_message(%Response{command: :declare_publisher} = response, conn) do
        {{pid, id}, conn} = Helpers.pop_request_tracker(conn, :declare_publisher, response.correlation_id)

        if pid != nil do
          GenServer.reply(pid, {:ok, id})
        end

        conn
      end

      defp handle_message(%Response{command: :query_publisher_sequence} = response, conn) do
        {{pid, _data}, conn} = Helpers.pop_request_tracker(conn, :query_publisher_sequence, response.correlation_id)

        if pid != nil do
          GenServer.reply(pid, {:ok, response.data.sequence})
        end

        conn
      end

      defp handle_message(%Response{command: :subscribe} = response, conn) do
        {{pid, data}, conn} = Helpers.pop_request_tracker(conn, :subscribe, response.correlation_id)

        {subscription_id, subscriber} = data

        if pid != nil do
          GenServer.reply(pid, {:ok, subscription_id})
        end

        %{conn | subscriptions: Map.put(conn.subscriptions, subscription_id, subscriber)}
      end

      defp handle_message(%Response{command: :unsubscribe} = response, conn) do
        {{pid, subscription_id}, conn} = Helpers.pop_request_tracker(conn, :unsubscribe, response.correlation_id)

        if pid != nil do
          GenServer.reply(pid, :ok)
        end

        %{conn | subscriptions: Map.drop(conn.subscriptions, [subscription_id])}
      end

      defp handle_message(%Response{command: command} = response, conn)
           when command in [:create_stream, :delete_stream, :delete_publisher] do
        {{pid, _data}, conn} = Helpers.pop_request_tracker(conn, command, response.correlation_id)

        if pid != nil do
          GenServer.reply(pid, :ok)
        end

        conn
      end
    end
  end
end

defmodule RabbitMQStream.Helpers.PublishingTracker do
  alias __MODULE__

  alias RabbitMQStream.Message.Data.{
    PublishErrorData,
    PublishConfirmData
  }

  defstruct sequence: 0,
            entries: %{},
            table: %{}

  def push(%PublishingTracker{} = tracker, id, expected, client) do
    ids =
      for publishing_id <- expected, into: %{} do
        {{id, publishing_id}, tracker.sequence}
      end

    table = Map.merge(tracker.table, ids)

    entries =
      Map.put(tracker.entries, tracker.sequence, %{
        id: id,
        client: client,
        expected: expected,
        responses: []
      })

    %{tracker | sequence: tracker.sequence + 1, entries: entries, table: table}
  end

  # def match(%PublishingTracker{} = tracker, %PublishErrorData{} = response) do
  # end

  def match(%PublishingTracker{} = tracker, %PublishConfirmData{} = response) do
    for publishing_id <- response.publishing_ids do
      {publishing_id, Map.get(tracker.table, {response.publisher_id, publishing_id})}
    end
    |> Enum.reduce(tracker, fn
      {_, nil}, tracker ->
        tracker

      {publishing_id, sequence}, tracker ->
        entries =
          Map.update!(tracker.entries, sequence, fn entry ->
            %{entry | responses: entry.responses ++ [{:ok, publishing_id}]}
          end)

        %{tracker | entries: entries}
    end)
  end

  def match(%PublishingTracker{} = tracker, %PublishErrorData{} = response) do
    for error <- response.errors do
      {error, Map.get(tracker.table, {response.publisher_id, error.publishing_id})}
    end
    |> Enum.reject(fn {nil, _} -> true end)
    |> Enum.reduce(tracker, fn
      {_, nil}, tracker ->
        tracker

      {error, sequence}, tracker ->
        entries =
          Map.update!(tracker.entries, sequence, fn entry ->
            %{entry | responses: entry.responses ++ [error]}
          end)

        %{tracker | entries: entries}
    end)
  end

  def pop_complete(%PublishingTracker{} = tracker) do
    complete =
      tracker.entries
      |> Map.to_list()
      |> Enum.filter(fn {_, entry} ->
        Enum.count(entry.expected) == Enum.count(entry.responses)
      end)

    if Enum.count(complete) > 0 do
      sequences = Enum.map(complete, fn {sequence, _} -> sequence end)

      table_keys =
        for {_, entry} <- complete,
            publishing_id <- entry.expected do
          {entry.id, publishing_id}
        end

      entries = Map.drop(tracker.entries, sequences)
      table = Map.drop(tracker.table, table_keys)
      tracker = %{tracker | entries: entries, table: table}
      complete = Enum.map(complete, fn {_, entry} -> entry end)

      {tracker, complete}
    else
      {tracker, []}
    end
  end
end

defmodule RabbitMQStream.Helpers do
  @moduledoc false

  defmacro match_codes({:%{}, _, codes}) do
    Enum.map(codes, fn {code, name} ->
      quote do
        def to_atom(unquote(code)) do
          unquote(name)
        end
      end
    end) ++
      Enum.map(codes, fn {code, name} ->
        quote do
          def from_atom(unquote(name)) do
            unquote(code)
          end
        end
      end)
  end
end

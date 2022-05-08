defmodule XColony.Helpers do
  defmacro match_codes(size: _, codes: {:%{}, _, codes} = ast) do
    [
      quote do
        @codes unquote(ast)
      end
    ]
    ++ Enum.map(codes, fn {code, name} ->
        module = name |> Atom.to_string() |> Macro.camelize()

        quote do
          "#{__MODULE__}.#{unquote(module)}"
          |> String.to_atom()
          |> defmodule(do: defstruct([code: unquote(code)]))
        end
    end)
    ++ Enum.map(codes, fn {code, name} ->
        module = name
        |> Atom.to_string()
        |> Macro.camelize()

        quote do
          @strt "#{__MODULE__}.#{unquote(module)}" |> String.to_atom()
          def decode(<<unquote(code)::unsigned-integer-size(16)>>) do
            struct(@strt)
          end
        end
    end)
  end
end

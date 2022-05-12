defmodule RabbitStream.Helpers do
  defmacro match_codes({:%{}, _, codes}) do
    Enum.map(codes, fn {code, name} ->
        module = "#{Atom.to_string(name)}"  |> Macro.camelize()

        quote do
          name = "#{__MODULE__}.#{unquote(module)}"
          |> String.to_atom()
          defmodule name do
            defstruct([code: unquote(code)])

            def code() do
              unquote(code)
            end
          end
        end
    end)
    ++ Enum.map(codes, fn {code, name} ->
        module = "#{Atom.to_string(name)}"  |> Macro.camelize()

        quote do
          @strt "#{__MODULE__}.#{unquote(module)}" |> String.to_atom()
          def decode(unquote(code)) do
            struct(@strt)
          end
        end
    end)
  end
end

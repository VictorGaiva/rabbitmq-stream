defmodule RabbitMQStream.MixProject do
  use Mix.Project

  @source_url "https://github.com/VictorGaiva/rabbitmq-stream"
  @version File.read!("VERSION")

  def project do
    [
      app: :rabbitmq_stream,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      package: package(),
      source_url: @source_url,
      deps: deps(),
      elixirc_options: [
        warnings_as_errors: true
      ],
      docs: [
        source_ref: "v#{@version}",
        main: "getting-started",
        extra_section: "GUIDES",
        assets: "guides/assets",
        formatters: ["html", "epub"],
        groups_for_modules: groups_for_modules(),
        extras: extras(),
        compilers: [:erlang] ++ Mix.compilers(),
        groups_for_extras: groups_for_extras()
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.28.4", only: :dev, runtime: false},
      {:jason, "~> 1.4.1", only: :test, runtime: false}
    ]
  end

  defp package do
    [
      description: "Elixir Client for RabbitMQ Streams Protocol",
      files: [
        "lib",
        "mix.exs",
        "README.md",
        "CHANGELOG.md",
        "VERSION",
        ".formatter.exs"
      ],
      maintainers: [
        "Victor Gaíva"
      ],
      licenses: ["MIT"],
      links: %{
        Changelog: "#{@source_url}/blob/master/CHANGELOG.md",
        GitHub: @source_url
      }
    ]
  end

  defp extras do
    [
      "guides/introduction/getting-started.md",
      "guides/tutorial/subscribing.md",
      "CHANGELOG.md"
    ]
  end

  defp groups_for_extras do
    [
      Introduction: ~r/guides\/introduction\/.*/,
      Tutorial: ~r/guides\/tutorial\/.*/,
      Topics: ~r/guides\/[^\/]+\.md/,
      Changelog: "CHANGELOG.md"
    ]
  end

  defp groups_for_modules do
    [
      Client: [
        RabbitMQStream.Connection,
        RabbitMQStream.Publisher,
        RabbitMQStream.Subscriber
      ],
      "Offset Tracking": [
        RabbitMQStream.Subscriber.OffsetTracking.Strategy,
        RabbitMQStream.Subscriber.OffsetTracking.CountStrategy,
        RabbitMQStream.Subscriber.OffsetTracking.IntervalStrategy
      ],
      "Flow Control": [
        RabbitMQStream.Subscriber.FlowControl.Strategy,
        RabbitMQStream.Subscriber.FlowControl.MessageCount
      ]
    ]
  end
end

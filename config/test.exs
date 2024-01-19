import Config

# Prevents the CI from being spammed with logs
config :logger, :level, :info

config :rabbitmq_stream, :defaults,
  connection: [
    port: 5553
  ]

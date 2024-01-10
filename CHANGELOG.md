# Changelog

## 0.4.0

Added support for RabbitMQ 3.13, with Route, Partitions and SuperStreams support.

### 0.4.0 Features

- Added support for `:route`, `:partitions`, `:consumerupdate`, `:exchangecommandversions`, `:streamstats`, `:createsuperstream` and `:deletesuperstream` commands.
- Added serialization options for encoding and decoding messages.
- TLS Support
- Functional `single-active-consumer` and `filter_value` properties for consumers

### 0.4.0 Changes

The 'Message' module tree was refactored to make all the Encoding and Decoding logic stay close to each other.

- Improved the cleanup logic for closing the connection.

### 0.4.0 Breaking Changes

- Renamed `RabbitMQStream.Subscriber` to `RabbitMQStream.Consumer`

## 0.3.0

Added an implementation for a stream Consumer, fixed bugs and improved the documentation.

### 0.3.0 Features

- Added the `:credit` command.
- Added `RabbitMQStream.Subscriber`, which subscribes to a stream, while tracking its offset and credit based on customizeable strategies.
- Added the possibility of globally configuring the default Connection for Publishers and Subscribers

### 0.3.0 Bug Fixes

- Fixed an issue where tcp packages with multiple commands where not being correctly parsed, and in reversed order

### 0.3.0 Changes

- `RabbitMQStream.Publisher` no longer calls `connect` on the Connection during its setup.
- Moved `RabbitMQStream.Publisher`'s setup logic into `handle_continue`, to prevent locking up the application startup.
- `RabbitMQStream.Publisher` no longer declares the stream if it doesn't exists.
- `RabbitMQStream.Publisher` module now can optionally declare a `before_start/2` callback, which is called before it calls `declare_publisher/2`, and can be used to create the stream if it doesn't exists.
- `RabbitMQStream.Connection` now buffers the requests while the connection is not yet `:open`.

### 0.3.0 Breaking Changes

- Subscription deliver messages are now in the format `{:chunk, %RabbitMQ.OsirisChunk{}}`.

## 0.2.1

Documentation and Configuration refactoring

- It is now possible to define the connection and subscriber parameters throught the `config.exs` file
- Documentation improvements, and examples

## 0.2.0

The main objective of this release is to remove the manually added code from `rabbitmq_stream_common`'s Erlang implementation of Encoding and Decoding logic, with frame buffering.

## 0.1.0

Initial release with the following features:

- Opening connection to RabbitMQ server
- Declaring a Stream
- Creating a Stream Publisher
- Subscribing to Stream Messages
- Initial Hex Release

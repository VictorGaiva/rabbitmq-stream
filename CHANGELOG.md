# Changelog

## 0.1.0

Initial release with the following features:

- Opening connection to RabbitMQ server
- Declaring a Stream
- Creating a Stream Publisher
- Subscribing to Stream Messages
- Initial Hex Release

## 0.2.0

The main objective of this release is to remove the manually added code from `rabbitmq_stream_common`'s Erlang implementation of Encoding and Decoding logic, with frame buffering.

## 0.2.1

Documentation and Configuration refactoring

- It is now possible to define the connection and subscriber parameters throught the `config.exs` file
- Documentation improvements, and examples

## 0.3.0

Added an implementation for a stream Subscriber, fixed bugs and improved the documentation.

### Features

- Added the `:credit` command.
- Added `RabbitMQStream.Subscriber`, which subscribes to a stream, while tracking its offset and credit based on customizeable strategies.

### Bug Fixes

- Fixed an issue where tcp packages with multiple commands where not being correctly parsed, and in reversed order

### Changes

- `RabbitMQStream.Publisher` no longer calls `connect` on the connection
- Moved `RabbitMQStream.Publisher`'s setup logic into `handle_continue`, so that the supervision tree doesn't lag at startup
- `RabbitMQStream.Publisher` no longer declares the stream if it doesn't exists. There are plans of creating a `before_connect` callback
  where one could add this type of setup logic to the module, but for now it expects the stream to already exist.

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

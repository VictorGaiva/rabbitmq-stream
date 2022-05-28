# Contributing

Thank you for contributing to the project. We'd love to see your
issues and pull requests.

If you're creating a pull request, please consider these suggestions:

Fork, then clone the repo:

    git clone git@github.com:your-username/rabbitmq-stream.git

Install the dependencies:

    mix deps.get

The tests uses the following default credentials:

    username: guest
    password: guest
    vhost:    /
    port:     5552

You can activate RabbitMQ Stream Plugin on your server, if not already active, with:

    rabbitmq-plugins enable rabbitmq_stream

Make sure the tests pass:

    mix test

Make your change. Add tests for your change. Make the tests pass:

    mix test

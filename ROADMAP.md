# Roadmap

## v0.1.0

Target Date: October 2022

The probably some room to improve the performance and the architecture in general. With the collected feedback this one is going to be focused on refactoring, were necessary.

- Benchmarks and Performance overall.

## v0.2.0

Target Date: December 2022

- Exploration of dynamic Connections supervision in a Client Module.
- Exploration of Cluster support, with [_Well-behaved_](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/#:~:text=Well%2Dbehaved%20Clients) connections.

## v0.x.0

Target Date: ???

- Route and Partition: Currently in experimental.

## v1.0

- Stable Client, with supervising multiple dynamic Connections, Subscriptions, and Publishers.
- [_Well-behaved_](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/#:~:text=Well%2Dbehaved%20Clients) Cluster connection
- Workaround for connecting throught [load balancers](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams#:~:text=Client%20Workaround%20With%20a%20Load%20Balancer)

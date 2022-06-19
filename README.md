## pqueue

Golang library for using Postgres as a generic job queue.

**under active development** - not quite ready for production use, but check back soon!

Often, async job queues are implemented using systems like Redis or RabbitMQ.
However, people have found success using PostgreSQL as the data layer for queue implementations, which has some nice benefits:

1. No additional operational overhead if you're already using postgres as an RDBMS for your application
2. The ability to perform robust analytical queries over queue history, that may be easily joined with other data in postgres


### Roadmap

- [ ] Basic queue behavior and helpers - push and pop, keep alive and timeouts, job types
- [ ] GraphQL service layer - expose queue as a GraphQL API
- [ ] gRPC service layer - expose queue as gRPC service
- [ ] HTTP service layer - expose queue as an HTTP API

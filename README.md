My Kafka Streams Prototype
=====

This project is designed to experiment with joining and splitting streams
using the Kafka Streams API.

Joining Streams
---

Having a single processor consuming 2+ kafka streams.

1. Having the same message format.

2. Having different message format.


Splitting Streams
---

Having a single processor emit messages to 2+ kafka streams.

1. Having the same message format

a. Each message sent to all output streams
b. Each message sent to specific streams
c. Messages sent to specific streams based on a criteria

2. Having different message format

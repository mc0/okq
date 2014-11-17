Redeque
=======

A simple go-based reliable event queueing with at-least-once support.

The underlying data storage is [Redis](http://redis.io) and the protocol is [RESP](http://redis.io/topics/protocol).
This allows swapping connecting directly to the service from any redis client.

Commands
--------
* QREGISTER queue [queue ...]

  optionally register the queues a consumer will pop from

* QRPOP queue

  Grab the right-most event from a queue

* QLPEEK queue

  Look at the contents of the left-most event in a queue

* QRPEEK queue

  See QLPEEK; except the right-most

* QREM queue eventId

  Remove an event from a queue. Should be done after the event is successfully consumed.

* QLPUSH queue eventId contents

  Add an event as the new left-most event in the queue

* QRPUSH queue eventId contents

  See QLPUSH; except the right-most. Usually for high-priority events.

* QSTATUS

  Get the status of all active queues on the system in the format: [queue] [total] [processing]

okQ
=======

A simple go-based reliable event queueing with at-least-once support.

The underlying data storage is [Redis](http://redis.io) and the protocol is [RESP](http://redis.io/topics/protocol).
This allows swapping connecting directly to the service from any redis client.

Commands
--------

OkQ is modeled as a left-to-right queue. Clients submit jobs on the left
side of the queue (with `QLPUSH`, and consumers read off the right side (with
`QRPOP`).

```
     QLPUSH                QRPOP
client -> [job job job job] -> consumer
```

* QREGISTER queue [queue ...]

  register the queues a consumer will block for on `qnotify`

  Returns `OK`

* QRPOP queue [EX seconds] [NOACK]

  Grab the right-most event from a queue and allow `EX seconds` (default 30) to
  QACK to it. If NOACK is set than it is not necessary to QACK.

  Returns an array-reply with the event's ID and contents or nil

* QLPEEK queue

  Look at the contents of the left-most event in a queue

  Returns an array-reply with the event's ID and contents or nil

* QRPEEK queue

  See QLPEEK; except the right-most

* QACK queue eventId

  Acknowledge an event has been successfully consumed, removing it from the
  queue permanently.

  Returns the number of events removed

* QLPUSH queue eventId contents

  Add an event as the new left-most event in the queue

  Returns `OK` on success

* QRPUSH queue eventId contents

  See QLPUSH; except the right-most. Usually for high-priority events.

* QNOTIFY timeout

  block for `timeout` seconds until an event is available on any registered queues

  Returns a queue's name or nil if no events were available.

* QSTATUS [queue ...]

  Get the status of the given queues (or all active queues, if none are given)
  on the system in the format: [queue] total: [total] processing: [processing]

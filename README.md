# okq

[![Join the chat at https://gitter.im/mc0/okq](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/mc0/okq?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A simple go-based reliable event queueing with at-least-once support.

The underlying data storage is [Redis](http://redis.io) and the protocol is [RESP](http://redis.io/topics/protocol).
This allows swapping connecting directly to the service from any redis client.

[![Build Status](https://travis-ci.org/mc0/okq.svg?branch=master)](https://travis-ci.org/mc0/okq)

## Commands

okq is modeled as a left-to-right queue. Clients submit events on the left
side of the queue (with `QLPUSH`, and consumers read off the right side (with
`QRPOP`).

```
     QLPUSH                QRPOP
client -> [event event event event] -> consumer
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

* QLPUSH queue eventId contents [NOBLOCK]

  Add an event as the new left-most event in the queue.

  NOBLOCK can be set to spawn a routine on the server which will push the event
  onto the queue in the background, returning to the client as quickly as
  possible.

  Returns `OK` on success

  Returns an error if `NOBLOCK` is set and the okq instance is too overloaded to
  handle the event in the background

* QRPUSH queue eventId contents [NOBLOCK]

  See QLPUSH; except the right-most. Usually for high-priority events.

* QNOTIFY timeout

  block for `timeout` seconds until an event is available on any registered queues

  Returns a queue's name or nil if no events were available.

* QFLUSH queue

  Removes all events from the given queue.

  Effictively makes it as if the given queue never existed. Any events in the
  process of being consumed from the given queue may still complete, but if they
  do not complete they will not be added back to the queue.

  Returns `OK` on success

* QSTATUS [queue ...]

  Get information about the given queues (or all active queues, if none are
  given) on the system.

  An array of arrays will be returned, for example:

  ```
  > QSTATUS foo bar
  1) 1) "foo"
     2) (integer) 2
     3) (integer) 1
     4) (integer) 3
  2) 1) "bar"
     2) (integer) 43
     3) (integer) 0
     4) (integer) 0
  ```

  The integer values returned indicate (respectively):

  * total - The number of events currently held by okq for the queue, both
    those that are awaiting a consumer and those which are actively held by a
    consumer

  * processing - The number of events for the queue which are being actively
    held by a consumer

  * consumers - The number of consumers currently registered for the queue

  The returned order will match the order of the queues given in the call. If no
  queues are given (and so information on all active queues is being returned)
  they will be returned in ascending alphabetical order

  *NOTE that there may in the future be more information returned in the
  sub-arrays returned by this call; do not assume that they will always be of
  length 4*

* QINFO [queue ...]

  Get human readable information about the given queues (or all active queues,
  if none are given) on the system in the format:

  This command effectively calls QSTATUS with the given arguments and returns
  its output in a nicely formatted way. The returned value will be an array of
  strings, one per queue, each formatted like so:

  ```
  > QINFO foo bar
  foo  total: 2   processing: 1  consumers: 3
  bar  total: 43  processing: 0  consumers: 0
  ```

  See QSTATUS for the meaning of `total`, `processing`, and `consumers`

  *NOTE that this output is intended to be read by humans and its format may
  change slightly everytime the command is called. For easily machine readable
  output of the same data see the QSTATUS command*

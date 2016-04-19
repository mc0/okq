# okq

[![Build Status](https://travis-ci.org/mc0/okq.svg?branch=master)](https://travis-ci.org/mc0/okq)

okq is a redis-backed queueing server with a focus on simplicity, both in code
and interface.

* At-least-once by default. At-most-once is supported by setting `EX` the
  parameter to `0` when consuming events. Once successfully submitted events
  interacted with atomically in redis; okq crashing will never result in loss of data.

* Clients talk to okq using the redis protocol. So if your language has a redis
  driver you already have an okq driver as well.

* Binary safe

* Supports a single redis instance, redis sentinel or a redis cluster

* Multiple okq instances can run on the same redis instance/cluster without
  knowing about each other, easing deployment

## Table of contents

* [Install](#install)
* [Configuration](#configuration)
* [Usage](#usage)
* [Commands](#commands)
  * [QLPUSH](#qlpush)
  * [QRPUSH](#qrpush)
  * [QLPEEK](#qlpeek)
  * [QRPEEK](#qrpeek)
  * [QRPOP](#qrpop)
  * [QACK](#qack)
  * [QREGISTER](#qregister)
  * [QNOTIFY](#qnotify)
  * [QFLUSH](#qflush)
  * [QSTATUS](#qstatus)
  * [QINFO](#qinfo)
* [Consumers](#consumers)


## Install

From within the okq project root:

    go get ./...
    go build

You'll now have an `okq` binary. To get started you can do `okq -h` to see
available options

## Configuration

okq can take in options on the command line, through environment variables, or
from a config file. All configuration parameters are available through any of
the three. The three methods can be mixed an matched, with environment variables
taking precedence over a config file, and command line arguments taking
precedence over environment variables.

    # See command line parameters and descriptions
    okq -h

    # Create a configuration file with default values pre-populated
    okq --example > okq.conf

    # Use configuration file
    okq --config okq.conf

    # Set --listen-addr argument in an environment variable (as opposed to on
    # the command line or the configuration file
    export OKQ_LISTEN_ADDR=127.0.0.1:4777
    okq

    # Mix all three
    export OKQ_LISTEN_ADDR=127.0.0.1:4777
    okq --config okq.conf --redis-cluster --redis-addr=127.0.0.1:6380

## Usage

By default okq listens on port 4777. You can connect to it using any existing
redis client, and all okq commands look very similar to redis commands. For
example, to peek at the next event on the `foo` queue:

```
redis-cli -p 4777
> QLPEEK foo
< 1) "1"
  2) "testevent"
```

See the next section for all available commands.

## Commands

okq is modeled as a left-to-right queue. Clients submit events on the left
side of the queue (with `QLPUSH`), and consumers read off the right side (with
`QRPOP`).

```
     QLPUSH                QRPOP
client -> [event event event event] -> consumer
```

It is not necessary to instantiate a queue. Simply pushing an event onto it or
creating a consumer for it implicitly creates it. Deleting a queue is also not
necessary, once a queue has no events and no consumers it no longer exists.

### QLPUSH

> QLPUSH queue eventID contents [NOBLOCK]

Add an event as the new left-most event in the queue.

eventID must be a unique string identifying the event. If an event with the
same eventID already exists in the queue an error will be returned. In most
cases a UUID will suffice.

This will not return with success until the event has been successfully stored
in redis. Set `NOBLOCK` if you want the server to return success as soon as
possible, even if the event can't be successfully added.

Returns `OK` on success

Returns an error if `NOBLOCK` is set and the okq instance is too overloaded to
handle the event in the background. Increasing `bg-push-pool-size` will
increase the number of available routines which can handle unblocked push
commands.

### QRPUSH

> QRPUSH queue eventId contents [NOBLOCK]

Behaves the same as [QLPUSH](#qlpush), except it pushes the event onto the
right side of the queue (the front). The event will be the next one consumed,
making this useful for one-off high priority events. However for a true high
priority queue it often makes more sense to have a second queue with its own
set of consumers than to use this.

### QLPEEK

> QLPEEK queue

Look at the left-most event in the queue, without actually consuming it.

Returns an array-reply with the eventID and contents of the left-most event, or
nil if the queue is empty.

```
> QLPEEK foo
< 1) "d7601248-ea90-4abc-a6e2-ff259f1205d1"
  2) "event contents"

> QLPEEK empty-queue
< (nil)
```

### QRPEEK

> QLPEEK queue

Behaves the same as [QLPEEK](#qlpeek), except it looks at the right-most event
on the queue (the one which will be consumed next).

### QRPOP

> QRPOP queue [EX seconds]

Pop the right-most event off the queue.

`EX seconds` determines how long the consumer has to [QACK](#qack) the event
before it is put back onto the right side of the queue (so it will be consumed
next) and made available to other consumers again. If not set, defaults to 30
seconds. If set to `0` the event will automatically be acknowledged.

Furthermore, when sending `EX 0` it is not necessary for the consumer to send
a corresponding [QACK](#qack). Setting this option allows you to make a
particular queue at-most-once (rather than at-least-once).

Returns an array-reply with the eventID and contents of the right-most event, or
nil if the queue is empty.

```
> QRPOP foo
< 1) "9919b6ba-298a-44ee-9127-7176e91fd7d7"
  2) "event contents to be consumed"

> QRPOP empty-queue
< (nil)
```

### QACK

> QACK queue eventID

Acknowledges that it is safe for okq to forget about this event for this queue.

If this is not called within some amount of time after popping the event off the
queue (see [QRPOP](#qrpop) for more on configuring that timeout) then the event
will be placed back onto the right side of the queue (so it will be consumed
next).

Returns an integer `1` if the event was acknowledged successfully, or `0` if not
(implying the event timed out or it was acknowledged by another consumer).

### QREGISTER

> QREGISTER [queue ...]

Register a client to zero or more queues. Used in conjunction with
[QNOTIFY](#qnotify).

Subsequent QREGISTER calls on the same client connection overwrites the queue
list from previous calls. Calling QREGISTER with no queues de-registers the
client from all queues. The client disconnecting also deregisters it from all
queues.

Once registered a client is considered a consumer and can call
[QNOTIFY](#qnotify) to block until an event is available on one of its
registered queues. The client being a consumer for these queues will also be
reflected in calls to [QSTATUS](#qstatus) and [QINFO](#qinfo).

Returns `OK`

### QNOTIFY

> QNOTIFY timeout

Block for `timeout` seconds until an event is available on any
[registered](#qregister) queue.

Returns a queue's name, or `nil` if no new events became available within the
timeout.

*NOTE This feature is supported using an internal redis pubsub channel.
Consequently, if an event is pushed to a queue on one instance of okq, another
instance of okq pointed at the same redis instance/cluster as the first will
still send the queue name to all relevant clients calling QNOTIFY.*

### QFLUSH

> QFLUSH queue

Removes all events from the given queue.

Effictively makes it as if the given queue never existed. Any events in the
process of being consumed from the given queue may still complete, but if they
do not complete succesfully (no [QACK](#qack) is received) they will not be
added back to the queue.

Returns `OK` on success

### QSTATUS

> QSTATUS [queue ...]

Get information about the given queues (or all active queues, if none are
given) on the system.

An array of arrays will be returned, for example:

```
> QSTATUS foo bar
< 1) 1) "foo"
     2) (integer) 2
     3) (integer) 1
     4) (integer) 3
  2) 1) "bar"
     2) (integer) 43
     3) (integer) 0
     4) (integer) 0
```

The integer values returned indicate (respectively):

* total - The number of events currently held by okq for the queue, both those
  that are awaiting a consumer and those which are actively held by a consumer

* processing - The number of events for the queue which are being actively held
  by a consumer

* consumers - The number of consumers currently registered for the queue

The returned order will match the order of the queues given in the call. If no
queues are given (and so information on all active queues is being returned)
they will be returned in ascending alphabetical order

*NOTE that there may in the future be more information returned in the
sub-arrays returned by this call; do not assume that they will always be of
length 4*

### QINFO

> QINFO [queue ...]

Get human readable information about the given queues (or all active queues,
if none are given) on the system in a human readable format.

This command effectively calls [QSTATUS](#qstatus) with the given arguments and
returns its output in a nicely formatted way. The returned value will be an
array of strings, one per queue, each formatted like so:

```
> QINFO foo bar
< 1) "foo  total: 2   processing: 1  consumers: 3"
  2) "bar  total: 43  processing: 0  consumers: 0"
```

See QSTATUS for the meaning of `total`, `processing`, and `consumers`

*NOTE that this output is intended to be read by humans and its format may
change slightly everytime the command is called. For easily machine readable
output of the same data see the [QSTATUS](#qstatus) command*

## Consumers

Writing okq clients (those which only submit events) is easy: A redis driver and
a call to [QLPUSH](#qlpush) are all that are needed.

The logic for consumer clients (those which are retrieving events and processing
them) is, however, slightly more involved (although not nearly as much
as for some other queue servers).

Here is the general order of events for a consumer for the `foo`, `bar`, and
`baz` queues:

1) Call `QREGISTER foo bar baz`. The client is now considered a consumer for
   these queues.

2) Call `QNOTIFY 30`. This will block until an event is pushed onto one of the
   three queues by some other client or the 30 second timeout is reached. If the
   timeout is reached `nil` will be returned and the consumer can start this
   step over. If an event was pushed then the name of the queue it was pushed
   onto will be returned from this call.

3) Call `QRPOP <queue from last step>`. If it returns `nil` then another
   consumer nabbed the event before we could; go back to step 2. Otherwise this
   will return the eventID and its contents.

4) At this point any application specific logic for the event should be run.
   Assuming success call `QACK <queue name from previous steps> <eventID>`. Go
   back to step 2.

It's likely that you'll want to write some generic wrapper code for this in your
language, if someone else hasnt written it already.

package consumers

import (
	"time"

	"github.com/mediocregopher/pubsubch"

	"github.com/mc0/okq/config"
	"github.com/mc0/okq/db"
	"github.com/mc0/okq/log"
)

// This piece runs separately from the main consumer thread. Its job is to
// subscribe to the notify redis channel and deal out the publishes it sees, as
// well as manage the subscription to list to make sure it's always subscribed
// to the correct channels

// Only need a buffer of size one. If two things are writing to the buffer, they
// both want subbed queues updated
var updateNotifyCh = make(chan struct{}, 1)

// This routine makes a connection to redis, and spawns of a subManager routine.
// That routine's job is to manage what channels this one's sub connection is
// actually subscribed to, while this one reads off the publish channel and
// passes those messages along
func subSpin() {
	for {
		subConn, err := pubsubch.DialTimeout(config.RedisAddr, 2500*time.Millisecond)
		if err != nil {
			log.L.Printf("notifyConsumers error connecting: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// When this is written to it will cause the subManager routine to
		// subscribe to new queue channels and unsubscribe from old ones as
		// necessary. We proxy all messages from updateNotifyCh to this one, so
		// we can close this one when we want to close the subManager
		triggerUpdateCh := make(chan struct{}, 1)
		go subManager(subConn, triggerUpdateCh)

	selectLoop:
		for {
			select {
			// pass the notification on to the subManager for processing
			case s := <-updateNotifyCh:
				select {
				case triggerUpdateCh <- s:
				default:
				}
			// listen for any publishes and fan them out to each client
			case pub, ok := <-subConn.PublishCh:
				if !ok {
					break selectLoop
				}
				queueName, err := db.GetQueueNameFromKey(pub.Channel)
				if err != nil {
					log.L.Printf("notifyConsumer got unknown channel %v: %v", pub.Channel, err)
					continue
				}
				clients := queueClients(queueName)
				for _, client := range clients {
					client.Notify(queueName)
				}
			}
		}

		close(triggerUpdateCh)
		subConn.Close()
	}
}

func subManager(subConn *pubsubch.PubSubCh, updateCh chan struct{}) {
	lastSubscribedQueues := []string{}
	// ensure we run immediately by filling the channel
	select {
	case updateCh <- struct{}{}:
	default:
	}

	for {
		if _, ok := <-updateCh; !ok {
			break
		}

		queueNames := registeredQueues()
		queuesAdded := stringSliceSub(queueNames, lastSubscribedQueues)
		queuesRemoved := stringSliceSub(lastSubscribedQueues, queueNames)

		lastSubscribedQueues = queueNames

		if len(queuesRemoved) != 0 {
			var redisChannels []string
			for i := range queuesRemoved {
				channelName := db.QueueChannelNameKey(queuesRemoved[i])
				redisChannels = append(redisChannels, channelName)
			}

			log.L.Debugf("unsubscribing from %v", redisChannels)
			if _, err := subConn.Unsubscribe(redisChannels...); err != nil {
				log.L.Printf("notifyConsumers error unsubscribing: %v", err)
				break
			}
		}

		if len(queuesAdded) != 0 {
			var redisChannels []string
			for i := range queuesAdded {
				channelName := db.QueueChannelNameKey(queuesAdded[i])
				redisChannels = append(redisChannels, channelName)
			}

			log.L.Debugf("subscribing to %v", redisChannels)
			if _, err := subConn.Subscribe(redisChannels...); err != nil {
				log.L.Printf("notifyConsumers error subscribing: %v", err)
				break
			}
		}
	}

	subConn.Close()
}

package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

const (
	stanName              = "example-stan"
	clientNamePrefix      = "client"
	natsKubeAddr          = "nats://example-nats:4222"
	natsSubject           = "foo1"
	natsDurableClientName = "my-durable"
	natsUsername          = "my-user"
	natsPassword          = "T0pS3cr3t"
	msgPostfix            = ""
)

var (
	clientCounter  int64
	messageCounter int64
)

// Simulate making a new ID
func randomID() string {
	return strings.ToUpper(strings.ReplaceAll(uuid.New().String(), "-", "")[0:17])
}

func nextClientNameFunc() string {
	return clientNamePrefix + "-" + strconv.FormatInt(atomic.AddInt64(&clientCounter, 1), 10)
}

func newMessageFunc() []byte {
	return []byte(strconv.FormatInt(atomic.AddInt64(&messageCounter, 1), 10))
}

var (
	doneChan = make(chan struct{}, 1)
)

func createPublisher(id string, sc stan.Conn, length, freq time.Duration) error {
	var err error

	defer func() {
		// Capture the error from above so that when we return it'll return
		err = sc.Close()

		doneChan <- struct{}{}
	}()

	var (
		ticker = time.NewTicker(freq)
		after  = time.After(length)
	)

	// For {{after}} seconds send a message to Nats every {{ticker}} seconds
	for {
		select {
		case <-after:
			ticker.Stop()
			return err

		case <-ticker.C:
			// fmt.Println("publish")

			// Simple Publisher
			err = sc.Publish(id, newMessageFunc())
			if err != nil {
				fmt.Println("sc.Publish")
				return err
			}
		}
	}
}

// // TODO: use a channel here that then publishes to other channels
// // This is all on a local Kubernetes installation in Kind; sometimes it is port forwarded
// func publish(nc *nats.Conn, length, freq time.Duration) error {
// 	var sc, err = stan.Connect(stanName, randomID(), stan.NatsConn(nc))
// 	if err != nil {
// 		fmt.Println("stan.Connect")
// 		return err
// 	}

// 	createPublisher(sc, length, freq)

// 	// TODO:
// 	return nil
// }

func startNats(num int, id string, total time.Duration) error {
	// Get cli flag here for kube or not
	var (
		natsURL = nats.DefaultURL
		creds   = nats.UserInfo(natsUsername, natsPassword)
	)

	// Connect to Nats
	nc, err := nats.Connect(natsURL, creds)
	if err != nil {
		fmt.Println("nats.Connect")
		return err
	}

	// Connect to Stan
	sc, err := stan.Connect(stanName, id, stan.NatsConn(nc))
	if err != nil {
		fmt.Println("stan.Connect")
		return err
	}

	// Save these two things to try out later
	// sub, err := nc.ChanSubscribe(clientName, )
	// sub.SetPendingLimits()

	// Make the client subscribe to a "mailbox" with the same name as the client ID
	sub, err := sc.Subscribe(id, func(m *stan.Msg) {
		fmt.Printf("%d: %s\n", num, string(m.Data))
	}, stan.DurableName("durable"+id))
	if err != nil {
		fmt.Println("sc.Subscribe")
		return err
	}

	log.Println("Client " + id + " subscribed!")

	// Simulate some time passing doing other things while we recieve messages
	time.Sleep(total)

	// Unsubscribe from the subject - un-interest the client from the subject
	err = sub.Unsubscribe()
	if err != nil {
		fmt.Println("sub.Unsubscribe")
		return err
	}

	// Close the connection to simulate a client going offline
	// Import to note here that the client does NOT de-interest from the subject
	err = sc.Close()
	if err != nil {
		fmt.Println("sub.Unsubscribe")
		return err
	}

	// Close connection
	return sc.Close()
}

func main() {
	// TODO: get CLI flag for
	// amount of clients
	// amount of senders
	//		AND
	// how often to send
	// how long to send for
	//		OR
	// how many total messages to send

	var (
		natsURL = nats.DefaultURL
		creds   = nats.UserInfo(natsUsername, natsPassword)
	)

	// Connect to Nats once for the publisher
	var nc, err = nats.Connect(natsURL, creds)
	if err != nil {
		fmt.Println("nats.Connect", err)
		return
	}

	sc, err := stan.Connect(stanName, "publisher", stan.NatsConn(nc))
	if err != nil {
		fmt.Println("stan.Connect")
		// return err
	}

	var (
		length = 10 * time.Second
		freq   = 100 * time.Millisecond
	)

	for i := 0; i < 1; i++ {
		go func(i int) {
			var id = randomID()

			go createPublisher(id, sc, length, freq)

			var err = startNats(i, id, length)
			if err != nil {
				log.Fatal("err", err)
			}
		}(i)
	}

	<-doneChan
}

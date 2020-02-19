package main

import (
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

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
	msgPostfix            = "Hello World"
)

var (
	clientCounter  int64
	messageCounter int64
)

func nextClientNameFunc() string {
	return clientNamePrefix + "-" + strconv.FormatInt(atomic.AddInt64(&clientCounter, 1), 10)
}

func newMessageFunc() []byte {
	return []byte(strconv.FormatInt(atomic.AddInt64(&messageCounter, 1), 10) + " " + msgPostfix)
}

// This is all on a local Kubernetes installation in Kind; sometimes it is port forwarded
func publish(nc *nats.Conn, length, freq time.Duration) error {
	var sc, err = stan.Connect(stanName, nextClientNameFunc(), stan.NatsConn(nc))
	if err != nil {
		fmt.Println("stan.Connect")
		return err
	}

	defer func() {
		// Capture the error from above so that when we return it'll return
		err = sc.Close()
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
			fmt.Println("publish")

			// Simple Publisher
			err = sc.Publish(natsSubject, newMessageFunc())
			if err != nil {
				fmt.Println("nc.Publish")
				return err
			}
		}
	}
}

func messageRecievedFunc(m *stan.Msg) {
	fmt.Printf("Received a message: %s\n", string(m.Data))
}

func startNats() error {
	// Get cli flag here for kube or not
	var (
		natsURL    = nats.DefaultURL
		clientName = nextClientNameFunc()
		creds      = nats.UserInfo(natsUsername, natsPassword)
	)

	nc, err := nats.Connect(natsURL, creds)
	if err != nil {
		fmt.Println("nats.Connect")
		return err
	}

	fmt.Printf("%+v\n", nc.Status())
	fmt.Println(nc.ConnectedServerId())

	sc, err := stan.Connect(stanName, clientName, stan.NatsConn(nc))
	if err != nil {
		fmt.Println("stan.Connect")
		return err
	}

	// Do some async publishing
	go publish(nc, 10*time.Second, 1*time.Second)

	// Simple Async Subscriber
	sub, err := sc.Subscribe(natsSubject, messageRecievedFunc, stan.DurableName(natsDurableClientName))
	if err != nil {
		fmt.Println("sc.Subscribe")
		return err
	}

	// Simulate some time passing doing other things while we recieve messages
	time.Sleep(3 * time.Second)

	// Close the connection to simulate a client going offline
	// Import to note here that the client does NOT de-interest from the subject
	err = sc.Close()
	if err != nil {
		fmt.Println("sub.Unsubscribe")
		return err
	}

	sc, err = stan.Connect(stanName, clientName, stan.NatsConn(nc))
	if err != nil {
		fmt.Println("stan.Connect")
		return err
	}

	time.Sleep(3 * time.Second)

	// Simple Async Subscriber
	sub, err = sc.Subscribe(natsSubject, messageRecievedFunc, stan.DurableName(natsDurableClientName))
	if err != nil {
		fmt.Println("sc.Subscribe")
		return err
	}

	// Simulate some time doing something else
	time.Sleep(6 * time.Second)

	// Unsubscribe from the subject - un-interest the client from the subject
	err = sub.Unsubscribe()
	if err != nil {
		fmt.Println("sub.Unsubscribe")
		return err
	}

	// Wait a bit again just to let things settle
	time.Sleep(2 * time.Second)

	// Close connection
	return sc.Close()
}

func main() {
	var err = startNats()
	if err != nil {
		log.Fatal("err", err)
	}
}

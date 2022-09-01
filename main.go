package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type client struct {
	ctx     context.Context
	queue   string
	message chan string
	wait    bool
}

type parcel struct {
	queue   string
	message string
}

type Broker struct {
	clients chan *client
	parcels chan *parcel

	messages       map[string][]string
	waitingClients map[string][]*client

	stop context.CancelFunc
	done chan struct{}
}

func NewBroker() *Broker {
	b := &Broker{
		parcels:        make(chan *parcel),
		clients:        make(chan *client),
		messages:       make(map[string][]string),
		waitingClients: make(map[string][]*client),
		done:           make(chan struct{}),
	}
	var ctx context.Context
	ctx, b.stop = context.WithCancel(context.Background())
	go b.run(ctx)
	return b
}

func (b *Broker) Push(ctx context.Context, queue, message string) error {
	p := &parcel{queue, message}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.parcels <- p:
		return nil
	}
}

func (b *Broker) Read(ctx context.Context, queue string, wait bool) (string, bool) {
	messageCh := make(chan string)
	c := &client{ctx, queue, messageCh, wait}

	select {
	case <-ctx.Done():
		return "", false
	case b.clients <- c:
	}

	select {
	case <-ctx.Done():
		return "", false
	case message, ok := <-messageCh:
		return message, ok
	}
}

func (b *Broker) Close(ctx context.Context) error {
	b.stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return nil
	}
}

func (b *Broker) addParcel(p *parcel) {
	listeners := b.waitingClients[p.queue]

	for i, lis := range listeners {
		select {
		case <-lis.ctx.Done():
		case lis.message <- p.message:
			close(lis.message)
			b.waitingClients[p.queue] = listeners[i+1:]
			return
		}
	}
	b.waitingClients[p.queue] = nil

	// No active listeners. Store message and quit.
	b.messages[p.queue] = append(b.messages[p.queue], p.message)
}

func (b *Broker) handleClient(c *client) {
	if c.wait {
		messages := b.messages[c.queue]
		if len(messages) > 0 {
			c.message <- messages[0]
			close(c.message)
			b.messages[c.queue] = messages[1:]
		} else {
			b.waitingClients[c.queue] = append(b.waitingClients[c.queue], c)
		}
		return
	}
	defer close(c.message)

	messages := b.messages[c.queue]
	if len(messages) < 1 {
		return
	}

	select {
	case c.message <- messages[0]:
		b.messages[c.queue] = messages[1:]
	case <-c.ctx.Done():
	}
}

func (b *Broker) run(ctx context.Context) {
	defer func() {
		close(b.parcels)
		close(b.clients)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case p := <-b.parcels:
			b.addParcel(p)
		case c := <-b.clients:
			b.handleClient(c)
		}
	}
}

type brokerHandler struct {
	broker *Broker
}

func (h *brokerHandler) Put(w http.ResponseWriter, r *http.Request) {
	queue := strings.TrimPrefix(r.URL.Path, "/")

	message := r.URL.Query().Get("v")
	if message == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err := h.broker.Push(r.Context(), queue, message)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (h *brokerHandler) Get(w http.ResponseWriter, r *http.Request) {
	queue := strings.TrimPrefix(r.URL.Path, "/")

	var (
		ctx  = r.Context()
		wait = false
	)
	if r.URL.Query().Has("timeout") {
		timeout, err := strconv.Atoi(r.URL.Query().Get("timeout"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		wait = true

		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
		defer cancel()
	}

	message, ok := h.broker.Read(ctx, queue, wait)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
	} else {
		io.WriteString(w, message)
	}
}

func main() {

	port := flag.Int("port", 8080, "port to start the server on")
	flag.Parse()
	if *port < 0 || *port > 65536 {
		*port = 8080
	}
	b := NewBroker()
	defer func() { b.Close(context.TODO()) }()
	h := brokerHandler{b}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			h.Put(w, r)
		case http.MethodGet:
			h.Get(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	http.ListenAndServe(":8081", nil)
}

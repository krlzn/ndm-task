package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// broker.go
//
//
// go run broker.go -p 3000
// -p Порт, опционально, по дефолту 3000
//
// GET handleGet->getQueue->tryGetWithTimeout
// Получаем очередь по ключу и пытаемся либо сразу отдать значение, либо создаем waiter до таймаута
//
// PUT handlePut->getQueue->put
// Получаем очередь по ключу(создаем при необходимости), пытаемся сразу отдать значение, минуя буфер,
// иначе добавляем значение в очередь
//
//
// Можно было бы ещё GC на очереди написать, но это будто бы уже лишнее относительно особенностей поставленной задачи

type queue struct {
	mu       sync.Mutex
	messages []string
	waiters  []*waiter
}

type waiter struct {
	ch chan string
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) getQueue(name string) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func (q *queue) put(message string) {
	q.mu.Lock()
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		q.mu.Unlock()
		w.ch <- message
		return
	}
	q.messages = append(q.messages, message)
	q.mu.Unlock()
}

func (q *queue) removeWaiter(w *waiter) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, cur := range q.waiters {
		if cur == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return true
		}
	}
	return false
}

func (q *queue) tryGetWithTimeout(ctx context.Context, timeout time.Duration) (string, bool) {
	q.mu.Lock()

	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.mu.Unlock()
		return msg, true
	}

	if timeout <= 0 {
		q.mu.Unlock()
		return "", false
	}

	w := &waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case msg := <-w.ch:
		return msg, true
	case <-ctx.Done():
		if q.removeWaiter(w) {
			return "", false
		}
		msg := <-w.ch
		return msg, true

	}
}

func (b *broker) handlePut(w http.ResponseWriter, r *http.Request, name string) {
	if !r.URL.Query().Has("v") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	message := r.URL.Query().Get("v")
	b.getQueue(name).put(message)
	w.WriteHeader(http.StatusOK)
}

func (b *broker) handleGet(w http.ResponseWriter, r *http.Request, name string) {
	var timeout time.Duration
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		timeout = time.Second * time.Duration(n)
	}
	msg, ok := b.getQueue(name).tryGetWithTimeout(r.Context(), timeout)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(msg)); err != nil {
		log.Println(err)
	}
}

func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.Trim(r.URL.Path, "/")
	if name == "" || strings.Contains(name, "/") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		b.handleGet(w, r, name)
	case http.MethodPut:
		b.handlePut(w, r, name)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	port := flag.Int("p", 3000, "Port")
	flag.Parse()
	addr := fmt.Sprintf(":%d", *port)

	b := newBroker()

	srv := &http.Server{
		Addr:        addr,
		Handler:     b,
		ReadTimeout: 30 * time.Second,
		IdleTimeout: 60 * time.Second,
	}

	log.Printf("listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}

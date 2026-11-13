/*
Copyright 2014 Workiva, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// spinCount is the number of times a put or get will retry before parking.
// Producers and consumers in tight loops usually see the state change within
// a few iterations, and spinning briefly is much cheaper than parking and
// waking a goroutine.  If the state does not change within this many
// iterations the operation parks, so an idle buffer consumes no CPU.
const spinCount = 64

// roundUp takes a uint64 greater than 0 and rounds it up to the next
// power of 2.
func roundUp(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

type node struct {
	position uint64
	data     interface{}
}

type nodes []node

// RingBuffer is a MPMC buffer that achieves threadsafety with CAS operations
// only.  A put on full or get on empty call will block until an item
// is put or retrieved.  Calling Dispose on the RingBuffer will unblock
// any blocked threads with an error.  This buffer is similar to the buffer
// described here: http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
// with some minor additions.
type RingBuffer struct {
	_padding0      [8]uint64
	queue          uint64
	_padding1      [8]uint64
	dequeue        uint64
	_padding2      [8]uint64
	mask, disposed uint64
	_padding3      [8]uint64
	nodes          nodes

	// The fields below only come into play when a put or get has to block.
	// The fast path above remains lock-free.
	lock    sync.Mutex
	signal  chan struct{}
	waiters int64
}

func (rb *RingBuffer) init(size uint64) {
	size = roundUp(size)
	rb.nodes = make(nodes, size)
	for i := uint64(0); i < size; i++ {
		rb.nodes[i] = node{position: i}
	}
	rb.mask = size - 1 // so we don't have to do this with every put/get operation
	rb.signal = make(chan struct{})
}

// notify wakes every goroutine blocked in a put or get.  The hot paths call it
// only after observing a non-zero waiter count so that the common, uncontended
// case pays nothing; it is safe to call unconditionally (as Dispose does).  The
// signal channel is closed and replaced so that all waiters observe the wakeup.
func (rb *RingBuffer) notify() {
	rb.lock.Lock()
	close(rb.signal)
	rb.signal = make(chan struct{})
	rb.lock.Unlock()
}

// wait parks the calling goroutine until the buffer state changes, the buffer
// is disposed, or, if timeout is positive, the timeout expires.  The blocked
// condition (a full buffer for a put, an empty buffer for a get) is evaluated
// while holding lock so that a concurrent notify cannot be missed.  It returns
// false if the timeout expired.
func (rb *RingBuffer) wait(full bool, timeout time.Duration) bool {
	rb.lock.Lock()
	// Register as a waiter before checking the state so a concurrent notify
	// cannot observe zero waiters and skip waking us.
	atomic.AddInt64(&rb.waiters, 1)
	signal := rb.signal
	blocked := atomic.LoadUint64(&rb.disposed) == 0
	if blocked {
		// Re-evaluate the exact condition the caller is waiting on, using the
		// slot sequence rather than the queue/dequeue counters.  Those
		// counters advance before the slot's position is published, so they
		// would report "ready" while the slot is still unavailable.
		if full {
			pos := atomic.LoadUint64(&rb.queue)
			seq := atomic.LoadUint64(&rb.nodes[pos&rb.mask].position)
			blocked = int64(seq-pos) < 0
		} else {
			pos := atomic.LoadUint64(&rb.dequeue)
			seq := atomic.LoadUint64(&rb.nodes[pos&rb.mask].position)
			blocked = int64(seq-(pos+1)) < 0
		}
	}
	if !blocked {
		atomic.AddInt64(&rb.waiters, -1)
		rb.lock.Unlock()
		return true
	}
	rb.lock.Unlock()

	ok := true
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-signal:
		case <-timer.C:
			ok = false
		}
	} else {
		<-signal
	}

	atomic.AddInt64(&rb.waiters, -1)
	return ok
}

// Put adds the provided item to the queue.  If the queue is full, this
// call will block until an item is added to the queue or Dispose is called
// on the queue.  An error will be returned if the queue is disposed.
func (rb *RingBuffer) Put(item interface{}) error {
	_, err := rb.put(item, false)
	return err
}

// Offer adds the provided item to the queue if there is space.  If the queue
// is full, this call will return false.  An error will be returned if the
// queue is disposed.
func (rb *RingBuffer) Offer(item interface{}) (bool, error) {
	return rb.put(item, true)
}

func (rb *RingBuffer) put(item interface{}, offer bool) (bool, error) {
	var n *node
	pos := atomic.LoadUint64(&rb.queue)
	spins := 0
L:
	for {
		if atomic.LoadUint64(&rb.disposed) == 1 {
			return false, ErrDisposed
		}

		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		// The subtraction must be interpreted as signed: a sequence that is
		// behind the current position means the slot still holds an item from
		// the previous lap, i.e. the buffer is full.
		switch dif := int64(seq - pos); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.queue, pos, pos+1) {
				break L
			}
		case dif < 0:
			// The buffer is full.
			if offer {
				return false, nil
			}

			// Spin briefly in the hope a get frees a slot immediately, as is
			// common in tight producer/consumer loops.  Park only if the state
			// does not change, so an idle buffer consumes no CPU.
			if spins < spinCount {
				spins++
				runtime.Gosched()
				pos = atomic.LoadUint64(&rb.queue)
				continue
			}

			rb.wait(true, 0)
			spins = 0
			pos = atomic.LoadUint64(&rb.queue)
		default:
			// Another producer claimed this position; retry with a fresh one.
			pos = atomic.LoadUint64(&rb.queue)
		}
	}

	n.data = item
	atomic.StoreUint64(&n.position, pos+1)
	if atomic.LoadInt64(&rb.waiters) > 0 {
		rb.notify() // wake any goroutines blocked in a get
	}
	return true, nil
}

// Get will return the next item in the queue.  This call will block
// if the queue is empty.  This call will unblock when an item is added
// to the queue or Dispose is called on the queue.  An error will be returned
// if the queue is disposed.
func (rb *RingBuffer) Get() (interface{}, error) {
	return rb.Poll(0)
}

// Poll will return the next item in the queue.  This call will block
// if the queue is empty.  This call will unblock when an item is added
// to the queue, Dispose is called on the queue, or the timeout is reached. An
// error will be returned if the queue is disposed or a timeout occurs. A
// non-positive timeout will block indefinitely.
func (rb *RingBuffer) Poll(timeout time.Duration) (interface{}, error) {
	var (
		n     *node
		pos   = atomic.LoadUint64(&rb.dequeue)
		start time.Time
		spins int
	)
	if timeout > 0 {
		start = time.Now()
	}
L:
	for {
		if atomic.LoadUint64(&rb.disposed) == 1 {
			return nil, ErrDisposed
		}

		n = &rb.nodes[pos&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		// As with put, the difference must be interpreted as signed: a
		// sequence behind pos+1 means the slot has not been filled yet, i.e.
		// the buffer is empty.
		switch dif := int64(seq - (pos + 1)); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.dequeue, pos, pos+1) {
				break L
			}
		case dif < 0:
			// The buffer is empty.  Spin briefly in the hope a put adds an
			// item immediately, as is common in tight producer/consumer loops.
			// Park only if the state does not change, so an idle buffer
			// consumes no CPU.
			if spins < spinCount {
				spins++
				runtime.Gosched()
				pos = atomic.LoadUint64(&rb.dequeue)
				continue
			}

			var remaining time.Duration
			if timeout > 0 {
				remaining = timeout - time.Since(start)
				if remaining <= 0 {
					return nil, ErrTimeout
				}
			}

			if !rb.wait(false, remaining) {
				return nil, ErrTimeout
			}
			spins = 0
			pos = atomic.LoadUint64(&rb.dequeue)
		default:
			// Another consumer claimed this position; retry with a fresh one.
			pos = atomic.LoadUint64(&rb.dequeue)
		}
	}
	data := n.data
	n.data = nil
	atomic.StoreUint64(&n.position, pos+rb.mask+1)
	if atomic.LoadInt64(&rb.waiters) > 0 {
		rb.notify() // wake any goroutines blocked in a put
	}
	return data, nil
}

// Len returns the number of items in the queue.
func (rb *RingBuffer) Len() uint64 {
	return atomic.LoadUint64(&rb.queue) - atomic.LoadUint64(&rb.dequeue)
}

// Cap returns the capacity of this ring buffer.
func (rb *RingBuffer) Cap() uint64 {
	return uint64(len(rb.nodes))
}

// Dispose will dispose of this queue and free any blocked threads
// in the Put and/or Get methods.  Calling those methods on a disposed
// queue will return an error.
func (rb *RingBuffer) Dispose() {
	atomic.CompareAndSwapUint64(&rb.disposed, 0, 1)
	rb.notify() // wake any blocked puts and gets so they can return ErrDisposed
}

// IsDisposed will return a bool indicating if this queue has been
// disposed.
func (rb *RingBuffer) IsDisposed() bool {
	return atomic.LoadUint64(&rb.disposed) == 1
}

// NewRingBuffer will allocate, initialize, and return a ring buffer
// with the specified size.
func NewRingBuffer(size uint64) *RingBuffer {
	rb := &RingBuffer{}
	rb.init(size)
	return rb
}

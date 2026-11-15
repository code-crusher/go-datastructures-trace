/*
Copyright 2016 Workiva, LLC

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

package slice

import "sync/atomic"

// CopyOnWriteSlice is a thread-safe slice inspired by Java's
// java.util.concurrent.CopyOnWriteArrayList. It is most efficient when the
// slice is read far more often than it is written: reads never block and do
// not require any synchronization, while each write copies the underlying
// array before modifying it.
//
// The zero value is not usable; construct one with NewCopyOnWriteSlice.
type CopyOnWriteSlice struct {
	data atomic.Value // stores []interface{}
}

// NewCopyOnWriteSlice returns a CopyOnWriteSlice initialized with the
// provided elements.
func NewCopyOnWriteSlice(elems ...interface{}) *CopyOnWriteSlice {
	c := &CopyOnWriteSlice{}
	c.data.Store(append([]interface{}(nil), elems...))
	return c
}

// Len returns the number of elements in the slice.
func (c *CopyOnWriteSlice) Len() int {
	return len(c.Snapshot())
}

// Snapshot returns a copy of the underlying array. The returned slice is a
// private snapshot and is safe to iterate over or mutate without affecting
// the CopyOnWriteSlice or any other readers.
func (c *CopyOnWriteSlice) Snapshot() []interface{} {
	return c.data.Load().([]interface{})
}

// Get returns the element at the provided index. It panics if the index is
// out of range, matching the behavior of the built-in slice.
func (c *CopyOnWriteSlice) Get(index int) interface{} {
	return c.Snapshot()[index]
}

// Append adds the provided elements to the end of the slice, copying the
// underlying array so existing readers are unaffected.
func (c *CopyOnWriteSlice) Append(elems ...interface{}) {
	src := c.Snapshot()
	dst := make([]interface{}, len(src), len(src)+len(elems))
	copy(dst, src)
	dst = append(dst, elems...)
	c.data.Store(dst)
}

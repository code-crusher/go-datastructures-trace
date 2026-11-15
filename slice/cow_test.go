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

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCopyOnWriteSlice(t *testing.T) {
	c := NewCopyOnWriteSlice(1, 2, 3)
	assert.Equal(t, 3, c.Len())
	assert.Equal(t, 2, c.Get(1))
}

func TestCopyOnWriteSliceAppend(t *testing.T) {
	c := NewCopyOnWriteSlice(1, 2)
	c.Append(3, 4)

	assert.Equal(t, 4, c.Len())
	assert.Equal(t, []interface{}{1, 2, 3, 4}, c.Snapshot())
}

func TestCopyOnWriteSliceSnapshotIsIsolated(t *testing.T) {
	c := NewCopyOnWriteSlice(1, 2)

	snap := c.Snapshot() // {1, 2}
	c.Append(3)          // c is now {1, 2, 3}
	snap[0] = 99         // mutate the old snapshot

	// The snapshot still sees the array as it was when captured, and
	// mutating it must not affect the CopyOnWriteSlice.
	assert.Equal(t, []interface{}{99, 2}, snap)
	assert.Equal(t, []interface{}{1, 2, 3}, c.Snapshot())
}

func TestCopyOnWriteSliceConcurrentReadWrite(t *testing.T) {
	c := NewCopyOnWriteSlice(0)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Append(j)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = c.Len()
				_ = c.Snapshot()
			}
		}()
	}

	wg.Wait()
	assert.NotZero(t, c.Len())
}

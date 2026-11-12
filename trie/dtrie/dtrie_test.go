/*
Copyright (c) 2016, Theodore Butler
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package dtrie

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultHasher(t *testing.T) {
	assert.Equal(t,
		defaultHasher(map[int]string{11234: "foo"}),
		defaultHasher(map[int]string{11234: "foo"}))
	assert.NotEqual(t, defaultHasher("foo"), defaultHasher("bar"))
}

func collisionHash(key interface{}) uint32 {
	return uint32(0xffffffff) // for testing collisions
}

func TestInsert(t *testing.T) {
	insertTest(t, defaultHasher, 10000)
	insertTest(t, collisionHash, 1000)
}

func insertTest(t *testing.T, hashfunc func(interface{}) uint32, count int) *node {
	n := emptyNode(0, 32)
	for i := 0; i < count; i++ {
		n = insert(n, &entry{hashfunc(i), i, i})
	}
	return n
}

func TestGet(t *testing.T) {
	getTest(t, defaultHasher, 10000)
	getTest(t, collisionHash, 1000)
}

func getTest(t *testing.T, hashfunc func(interface{}) uint32, count int) {
	n := insertTest(t, hashfunc, count)
	for i := 0; i < count; i++ {
		x := get(n, hashfunc(i), i)
		assert.Equal(t, i, x.Value())
	}
}

func TestRemove(t *testing.T) {
	removeTest(t, defaultHasher, 10000)
	removeTest(t, collisionHash, 1000)
}

func removeTest(t *testing.T, hashfunc func(interface{}) uint32, count int) {
	n := insertTest(t, hashfunc, count)
	for i := 0; i < count; i++ {
		n = remove(n, hashfunc(i), i)
	}
	for _, e := range n.entries {
		if e != nil {
			t.Fatal("final node is not empty")
		}
	}
}

func TestUpdate(t *testing.T) {
	updateTest(t, defaultHasher, 10000)
	updateTest(t, collisionHash, 1000)
}

func updateTest(t *testing.T, hashfunc func(interface{}) uint32, count int) {
	n := insertTest(t, hashfunc, count)
	for i := 0; i < count; i++ {
		n = insert(n, &entry{hashfunc(i), i, -i})
	}
}

func TestIterate(t *testing.T) {
	n := insertTest(t, defaultHasher, 10000)
	echan := iterate(n, nil)
	c := 0
	for _ = range echan {
		c++
	}
	assert.Equal(t, 10000, c)
	// test with stop chan
	c = 0
	stop := make(chan struct{})
	echan = iterate(n, stop)
	for _ = range echan {
		c++
		if c == 100 {
			close(stop)
			break
		}
	}
	assert.True(t, c == 100)
	// test with collisions
	n = insertTest(t, collisionHash, 1000)
	c = 0
	echan = iterate(n, nil)
	for _ = range echan {
		c++
	}
	assert.Equal(t, 1000, c)
}

func TestSize(t *testing.T) {
	n := insertTest(t, defaultHasher, 10000)
	d := &Dtrie{n, defaultHasher}
	assert.Equal(t, 10000, d.Size())
}

func TestPersistentInsert(t *testing.T) {
	t0 := New(nil)
	t1 := t0.Insert("a", "b")
	t2 := t1.Insert("c", "d")

	assert.Equal(t, 0, t0.Size())
	assert.Equal(t, 1, t1.Size())
	assert.Equal(t, 2, t2.Size())

	assert.Nil(t, t0.Get("a"))
	assert.Nil(t, t0.Get("c"))
	assert.Equal(t, "b", t1.Get("a"))
	assert.Nil(t, t1.Get("c"))
	assert.Equal(t, "b", t2.Get("a"))
	assert.Equal(t, "d", t2.Get("c"))
}

func TestPersistentUpdate(t *testing.T) {
	t0 := New(nil).Insert("a", "b")
	t1 := t0.Insert("a", "c")

	assert.Equal(t, "b", t0.Get("a"))
	assert.Equal(t, "c", t1.Get("a"))
}

func TestPersistentRemove(t *testing.T) {
	t0 := New(nil).Insert("a", "b")
	t1 := t0.Remove("a")

	assert.Equal(t, "b", t0.Get("a"))
	assert.Equal(t, 1, t0.Size())
	assert.Nil(t, t1.Get("a"))
	assert.Equal(t, 0, t1.Size())
}

func TestPersistentInsertWithHashCollisions(t *testing.T) {
	t0 := New(collisionHash)
	t1 := t0.Insert("a", "b")
	t2 := t1.Insert("c", "d")

	assert.Equal(t, 0, t0.Size())
	assert.Equal(t, 1, t1.Size())
	assert.Equal(t, 2, t2.Size())

	assert.Nil(t, t0.Get("c"))
	assert.Nil(t, t1.Get("c"))
	assert.Equal(t, "d", t2.Get("c"))
}

func TestPersistentRemoveWithHashCollisions(t *testing.T) {
	t0 := New(collisionHash).Insert("a", "b").Insert("c", "d")
	t1 := t0.Remove("a")

	assert.Equal(t, "b", t0.Get("a"))
	assert.Equal(t, "d", t0.Get("c"))
	assert.Equal(t, 2, t0.Size())
	assert.Nil(t, t1.Get("a"))
	assert.Equal(t, "d", t1.Get("c"))
	assert.Equal(t, 1, t1.Size())
}

func TestGetMissingKeyInOccupiedSlot(t *testing.T) {
	// 0 and 32 share an index at level 0. Looking up a key that was never
	// inserted must return nil rather than the unrelated entry in that slot.
	d := New(nil).Insert(0, "zero")

	assert.Nil(t, d.Get(32))
	assert.Equal(t, "zero", d.Get(0))
}

func TestRemoveMissingKeyInOccupiedSlot(t *testing.T) {
	d := New(nil).Insert(0, "zero")
	d = d.Remove(32)

	assert.Equal(t, "zero", d.Get(0))
	assert.Equal(t, 1, d.Size())
}

func TestRemoveCompressesSubNode(t *testing.T) {
	// 0 and 32 share the same index at level 0, forcing a sub-node.
	d := New(nil).Insert(0, "zero").Insert(32, "thirty-two")
	d = d.Remove(0)

	assert.Nil(t, d.Get(0))
	assert.Equal(t, "thirty-two", d.Get(32))
	assert.Equal(t, 1, d.Size())
}

func TestConcurrentInsert(t *testing.T) {
	base := New(nil)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			d := base
			for i := 0; i < 1000; i++ {
				d = d.Insert(i, g)
			}
			assert.Equal(t, 1000, d.Size())
			for i := 0; i < 1000; i++ {
				assert.Equal(t, g, d.Get(i))
			}
		}(g)
	}
	wg.Wait()

	// The original trie must be unaffected by any of the inserts.
	assert.Equal(t, 0, base.Size())
}

func BenchmarkInsert(b *testing.B) {
	b.ReportAllocs()
	n := emptyNode(0, 32)
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		n = insert(n, &entry{defaultHasher(i), i, i})
	}
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()
	n := insertTest(nil, defaultHasher, b.N)
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		get(n, defaultHasher(i), i)
	}
}

func BenchmarkRemove(b *testing.B) {
	b.ReportAllocs()
	n := insertTest(nil, defaultHasher, b.N)
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		n = remove(n, defaultHasher(i), i)
	}
}

func BenchmarkUpdate(b *testing.B) {
	b.ReportAllocs()
	n := insertTest(nil, defaultHasher, b.N)
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		n = insert(n, &entry{defaultHasher(i), i, -i})
	}
}

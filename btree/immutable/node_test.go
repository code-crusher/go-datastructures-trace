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

package btree

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// newID replaced github.com/satori/go.uuid's NewV4().Bytes(), whose signature
// changed to return an error and broke this package's build.  It must keep
// returning 16 random bytes.
func TestNewID(t *testing.T) {
	id := newID()
	assert.Len(t, id, 16)

	other := newID()
	assert.Len(t, other, 16)
	assert.False(t, bytes.Equal(id, other))
}

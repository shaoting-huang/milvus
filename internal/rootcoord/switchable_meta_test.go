// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// fakeSwitchMeta is a lightweight IMetaTable used only by the switch test. It embeds a nil
// IMetaTable (so it satisfies the full interface) and overrides just two methods to make
// routing observable: CreateDatabase records that it was called, GetDatabaseByName returns
// a database tagged with this fake's name.
type fakeSwitchMeta struct {
	IMetaTable
	tag           string
	createDBCalls int
}

func (f *fakeSwitchMeta) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	f.createDBCalls++
	return nil
}

func (f *fakeSwitchMeta) GetDatabaseByName(ctx context.Context, dbName string, ts typeutil.Timestamp) (*model.Database, error) {
	return &model.Database{Name: f.tag}, nil
}

func TestSwitchableMetaTable_RoutesToCurrentTarget(t *testing.T) {
	ctx := context.Background()
	fakeA := &fakeSwitchMeta{tag: "A"}
	fakeB := &fakeSwitchMeta{tag: "B"}

	sw := NewSwitchableMetaTable(fakeA)

	// Current() returns the initial target.
	assert.Same(t, fakeA, sw.Current())

	// A call routes to A.
	err := sw.CreateDatabase(ctx, &model.Database{}, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, fakeA.createDBCalls)
	assert.Equal(t, 0, fakeB.createDBCalls)

	db, err := sw.GetDatabaseByName(ctx, "ignored", 0)
	assert.NoError(t, err)
	assert.Equal(t, "A", db.Name)

	// After Switch, the same calls route to B.
	sw.Switch(fakeB)
	assert.Same(t, fakeB, sw.Current())

	err = sw.CreateDatabase(ctx, &model.Database{}, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, fakeA.createDBCalls) // unchanged
	assert.Equal(t, 1, fakeB.createDBCalls)

	db, err = sw.GetDatabaseByName(ctx, "ignored", 0)
	assert.NoError(t, err)
	assert.Equal(t, "B", db.Name)
}

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

// Package mocks now only re-exports the KV testify mocks that live in the
// shared pkg/v3/kv/mocks package. The mocks were moved into pkg/v3 so the KV
// catalog tests (which also moved into pkg/v3) can reuse them from the same
// module. These aliases keep every existing internal/kv/mocks import site
// (datacoord / querycoord / streamingnode / rootcoord tests, ...) working
// unchanged.
package mocks

import (
	mocks "github.com/milvus-io/milvus/pkg/v3/kv/mocks"
)

// Mock types re-exported from pkg/v3/kv/mocks.
type (
	MetaKv                                    = mocks.MetaKv
	MetaKv_Close_Call                         = mocks.MetaKv_Close_Call
	MetaKv_CompareVersionAndSwap_Call         = mocks.MetaKv_CompareVersionAndSwap_Call
	MetaKv_Expecter                           = mocks.MetaKv_Expecter
	MetaKv_GetPath_Call                       = mocks.MetaKv_GetPath_Call
	MetaKv_Has_Call                           = mocks.MetaKv_Has_Call
	MetaKv_HasPrefix_Call                     = mocks.MetaKv_HasPrefix_Call
	MetaKv_Load_Call                          = mocks.MetaKv_Load_Call
	MetaKv_LoadWithPrefix_Call                = mocks.MetaKv_LoadWithPrefix_Call
	MetaKv_MultiLoad_Call                     = mocks.MetaKv_MultiLoad_Call
	MetaKv_MultiRemove_Call                   = mocks.MetaKv_MultiRemove_Call
	MetaKv_MultiSaveAndRemove_Call            = mocks.MetaKv_MultiSaveAndRemove_Call
	MetaKv_MultiSaveAndRemoveWithPrefix_Call  = mocks.MetaKv_MultiSaveAndRemoveWithPrefix_Call
	MetaKv_MultiSave_Call                     = mocks.MetaKv_MultiSave_Call
	MetaKv_Remove_Call                        = mocks.MetaKv_Remove_Call
	MetaKv_RemoveWithPrefix_Call              = mocks.MetaKv_RemoveWithPrefix_Call
	MetaKv_Save_Call                          = mocks.MetaKv_Save_Call
	MetaKv_WalkWithPrefix_Call                = mocks.MetaKv_WalkWithPrefix_Call
	TxnKV                                     = mocks.TxnKV
	TxnKV_Close_Call                          = mocks.TxnKV_Close_Call
	TxnKV_Expecter                            = mocks.TxnKV_Expecter
	TxnKV_Has_Call                            = mocks.TxnKV_Has_Call
	TxnKV_HasPrefix_Call                      = mocks.TxnKV_HasPrefix_Call
	TxnKV_Load_Call                           = mocks.TxnKV_Load_Call
	TxnKV_LoadWithPrefix_Call                 = mocks.TxnKV_LoadWithPrefix_Call
	TxnKV_MultiLoad_Call                      = mocks.TxnKV_MultiLoad_Call
	TxnKV_MultiRemove_Call                    = mocks.TxnKV_MultiRemove_Call
	TxnKV_MultiSaveAndRemove_Call             = mocks.TxnKV_MultiSaveAndRemove_Call
	TxnKV_MultiSaveAndRemoveWithPrefix_Call   = mocks.TxnKV_MultiSaveAndRemoveWithPrefix_Call
	TxnKV_MultiSave_Call                      = mocks.TxnKV_MultiSave_Call
	TxnKV_Remove_Call                         = mocks.TxnKV_Remove_Call
	TxnKV_RemoveWithPrefix_Call               = mocks.TxnKV_RemoveWithPrefix_Call
	TxnKV_Save_Call                           = mocks.TxnKV_Save_Call
	WatchKV                                   = mocks.WatchKV
	WatchKV_Close_Call                        = mocks.WatchKV_Close_Call
	WatchKV_CompareVersionAndSwap_Call        = mocks.WatchKV_CompareVersionAndSwap_Call
	WatchKV_Expecter                          = mocks.WatchKV_Expecter
	WatchKV_GetPath_Call                      = mocks.WatchKV_GetPath_Call
	WatchKV_Has_Call                          = mocks.WatchKV_Has_Call
	WatchKV_HasPrefix_Call                    = mocks.WatchKV_HasPrefix_Call
	WatchKV_Load_Call                         = mocks.WatchKV_Load_Call
	WatchKV_LoadWithPrefix_Call               = mocks.WatchKV_LoadWithPrefix_Call
	WatchKV_MultiLoad_Call                    = mocks.WatchKV_MultiLoad_Call
	WatchKV_MultiRemove_Call                  = mocks.WatchKV_MultiRemove_Call
	WatchKV_MultiSaveAndRemove_Call           = mocks.WatchKV_MultiSaveAndRemove_Call
	WatchKV_MultiSaveAndRemoveWithPrefix_Call = mocks.WatchKV_MultiSaveAndRemoveWithPrefix_Call
	WatchKV_MultiSave_Call                    = mocks.WatchKV_MultiSave_Call
	WatchKV_Remove_Call                       = mocks.WatchKV_Remove_Call
	WatchKV_RemoveWithPrefix_Call             = mocks.WatchKV_RemoveWithPrefix_Call
	WatchKV_Save_Call                         = mocks.WatchKV_Save_Call
	WatchKV_WalkWithPrefix_Call               = mocks.WatchKV_WalkWithPrefix_Call
	WatchKV_Watch_Call                        = mocks.WatchKV_Watch_Call
	WatchKV_WatchWithPrefix_Call              = mocks.WatchKV_WatchWithPrefix_Call
	WatchKV_WatchWithRevision_Call            = mocks.WatchKV_WatchWithRevision_Call
)

// Re-exported constructors.
var (
	NewMetaKv  = mocks.NewMetaKv
	NewTxnKV   = mocks.NewTxnKV
	NewWatchKV = mocks.NewWatchKV
)

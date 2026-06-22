package migration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	tilib "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Migration copies RootCoord metadata from the cluster's source backend (etcd) into the
// pooled catalog service's TiKV backend. Tests use a REAL etcd (ETCD_ENDPOINTS, default
// 127.0.0.1:2379) as source and a TiKV (in-process mock by default; TIKV_PD for a real
// cluster) as destination — the same backends the real cutover moves between.

var (
	etcdCli  *clientv3.Client
	tikvConn *txnkv.Client
)

func TestMain(m *testing.M) {
	paramtable.Init()
	etcdCli = setupEtcd()
	tikvConn = setupTiKV()
	os.Exit(m.Run())
}

func setupEtcd() *clientv3.Client {
	ep := os.Getenv("ETCD_ENDPOINTS")
	if ep == "" {
		ep = "127.0.0.1:2379"
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(ep, ","), DialTimeout: 5 * time.Second})
	if err != nil {
		panic(err)
	}
	return cli
}

func setupTiKV() *txnkv.Client {
	if pd := os.Getenv("TIKV_PD"); pd != "" {
		cli, err := txnkv.NewClient([]string{pd})
		if err != nil {
			panic(err)
		}
		return cli
	}
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	if err != nil {
		panic(err)
	}
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tilib.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	if err != nil {
		panic(err)
	}
	return &txnkv.Client{KVStore: store}
}

// newSrcKV returns a source (etcd) MetaKv over a fresh root prefix.
func newSrcKV(t *testing.T, root string) kv.MetaKv {
	src := etcdkv.NewEtcdKV(etcdCli, root)
	require.NoError(t, src.RemoveWithPrefix(context.Background(), ""))
	return src
}

// newDstKV returns a destination (TiKV) MetaKv over a fresh root prefix.
func newDstKV(t *testing.T, root string) kv.MetaKv {
	dst := tikvkv.NewTiKV(tikvConn, root)
	require.NoError(t, dst.RemoveWithPrefix(context.Background(), ""))
	return dst
}

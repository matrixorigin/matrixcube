package prophet

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
)

var (
	etcdTimeout = time.Second * 3
)

func startEmbedEtcd(ctx context.Context, cfg *config.Config) (*clientv3.Client, *embed.Etcd, error) {
	etcdCfg, err := cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, nil, err
	}

	newCtx, cancel := context.WithTimeout(ctx, option.EtcdStartTimeout)
	defer cancel()

	etcd, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return nil, nil, err
	}

	// Check cluster ID
	urlMap, err := types.NewURLsMap(cfg.EmbedEtcd.InitialCluster)
	if err != nil {
		return nil, nil, err
	}

	if err = util.CheckClusterID(etcd.Server.Cluster().ID(), urlMap); err != nil {
		return nil, nil, err
	}

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case <-newCtx.Done():
		return nil, nil, errors.New("context cancaled")
	}

	endpoints := []string{etcdCfg.ACUrls[0].String()}
	util.GetLogger().Infof("create etcd v3 client with endpoints %+v", endpoints)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
		AutoSyncInterval: time.Second * 30,
		DialTimeout:      etcdTimeout,
	})
	if err != nil {
		return nil, nil, err
	}

	etcdServerID := uint64(etcd.Server.ID())
	// update advertise peer urls.
	etcdMembers, err := util.ListEtcdMembers(client)
	if err != nil {
		return nil, nil, err
	}
	for _, m := range etcdMembers.Members {
		if etcdServerID == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if cfg.EmbedEtcd.AdvertisePeerUrls != etcdPeerURLs {
				util.GetLogger().Infof("update advertise peer urls %+v to %+v",
					cfg.EmbedEtcd.AdvertisePeerUrls,
					etcdPeerURLs)
				cfg.EmbedEtcd.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}

	return client, etcd, nil
}

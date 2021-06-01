package join

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver"
)

const (
	// privateFileMode grants owner to read/write a file.
	privateFileMode = 0600
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode = 0700
)

// listMemberRetryTimes is the retry times of list member.
var listMemberRetryTimes = 20

// PrepareJoinCluster sends MemberAdd command to Prophet cluster,
// and returns the initial configuration of the Prophet cluster.
//
// TL;TR: The join functionality is safe. With data, join does nothing, w/o data
//        and it is not a member of cluster, join does MemberAdd, it returns an
//        error if Prophet tries to join itself, missing data or join a duplicated Prophet.
//
// Etcd automatically re-joins the cluster if there is a data directory. So
// first it checks if there is a data directory or not. If there is, it returns
// an empty string (etcd will get the correct configurations from the data
// directory.)
//
// If there is no data directory, there are following cases:
//
//  - A new Prophet joins an existing cluster.
//      What join does: MemberAdd, MemberList, then generate initial-cluster.
//
//  - A failed Prophet re-joins the previous cluster.
//      What join does: return an error. (etcd reports: raft log corrupted,
//                      truncated, or lost?)
//
//  - A deleted Prophet joins to previous cluster.
//      What join does: MemberAdd, MemberList, then generate initial-cluster.
//                      (it is not in the member list and there is no data, so
//                       we can treat it as a new Prophet.)
//
// If there is a data directory, there are following special cases:
//
//  - A failed Prophet tries to join the previous cluster but it has been deleted
//    during its downtime.
//      What join does: return "" (etcd will connect to other peers and find
//                      that the Prophet itself has been removed.)
//
//  - A deleted Prophet joins the previous cluster.
//      What join does: return "" (as etcd will read data directory and find
//                      that the Prophet itself has been removed, so an empty string
//                      is fine.)
func PrepareJoinCluster(cfg *config.Config) {
	// - A Prophet tries to join itself.
	if cfg.EmbedEtcd.Join == "" {
		return
	}

	if cfg.EmbedEtcd.Join == cfg.EmbedEtcd.AdvertiseClientUrls {
		util.GetLogger().Fatalf("join self is forbidden")
	}

	filePath := path.Join(cfg.DataDir, "join")
	// Read the persist join config
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		s, err := ioutil.ReadFile(filePath)
		if err != nil {
			util.GetLogger().Fatalf("read the join config failed with %+v",
				err)
		}
		cfg.EmbedEtcd.InitialCluster = strings.TrimSpace(string(s))
		cfg.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting
		return
	}

	initialCluster := ""
	// Cases with data directory.
	if isDataExist(path.Join(cfg.DataDir, "member")) {
		cfg.EmbedEtcd.InitialCluster = initialCluster
		cfg.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting
		return
	}

	// Below are cases without data directory.
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.EmbedEtcd.Join, ","),
		DialTimeout: option.DefaultDialTimeout,
	})
	if err != nil {
		util.GetLogger().Fatalf("create etcd client failed with %+v",
			err)
	}
	defer client.Close()

	listResp, err := util.ListEtcdMembers(client)
	if err != nil {
		util.GetLogger().Fatalf("list embed etcd members failed with %+v",
			err)
	}

	existed := false
	for _, m := range listResp.Members {
		if len(m.Name) == 0 {
			util.GetLogger().Fatalf("there is a member that has not joined successfully",
				err)
		}
		if m.Name == cfg.Name {
			existed = true
		}
	}

	// - A failed Prophet re-joins the previous cluster.
	if existed {
		util.GetLogger().Fatalf("missing data or join a duplicated prophet")
	}

	var addResp *clientv3.MemberAddResponse
	// - A new Prophet joins an existing cluster.
	// - A deleted Prophet joins to previous cluster.
	{
		for {
			// First adds member through the API
			addResp, err = util.AddEtcdMember(client, []string{cfg.EmbedEtcd.AdvertisePeerUrls})
			if err != nil && err.Error() != etcdserver.ErrUnhealthy.Error() {
				util.GetLogger().Fatalf("add member to embed etcd failed with %+v", err)
			}

			if err != nil && err.Error() == etcdserver.ErrUnhealthy.Error() {
				util.GetLogger().Errorf("add member to embed etcd failed with %+v, retry later", err)
				time.Sleep(time.Millisecond * 500)
				continue
			}

			break
		}

	}

	var (
		prophets []string
		listSucc bool
	)

	for i := 0; i < listMemberRetryTimes; i++ {
		listResp, err = util.ListEtcdMembers(client)
		if err != nil {
			util.GetLogger().Fatalf("list embed etcd members failed with %+v",
				err)
		}

		prophets = []string{}
		for _, memb := range listResp.Members {
			n := memb.Name
			if addResp != nil && memb.ID == addResp.Member.ID {
				n = cfg.Name
				listSucc = true
			}
			if len(n) == 0 {
				util.GetLogger().Fatalf("there is a member that has not joined successfully",
					err)
			}
			for _, m := range memb.PeerURLs {
				prophets = append(prophets, fmt.Sprintf("%s=%s", n, m))
			}
		}

		if listSucc {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !listSucc {
		util.GetLogger().Fatalf("join failed, adds the new member %s may failed",
			cfg.Name)
	}

	initialCluster = strings.Join(prophets, ",")
	cfg.EmbedEtcd.InitialCluster = initialCluster
	cfg.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting
	err = os.MkdirAll(cfg.DataDir, privateDirMode)
	if err != nil && !os.IsExist(err) {
		util.GetLogger().Fatalf("create data path failed with %+v",
			err)
	}

	err = ioutil.WriteFile(filePath, []byte(cfg.EmbedEtcd.InitialCluster), privateFileMode)
	if err != nil {
		util.GetLogger().Fatalf("write data path failed with %+v",
			err)
	}

}

func isDataExist(d string) bool {
	dir, err := os.Open(d)
	if err != nil {
		util.GetLogger().Errorf("open directory %s failed with %+v", d, err)
		return false
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		util.GetLogger().Errorf("list directory %s failed with %+v", d, err)
		return false
	}
	return len(names) != 0
}

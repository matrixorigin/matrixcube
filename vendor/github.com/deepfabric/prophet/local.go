package prophet

import (
	"encoding/binary"
)

// local is in (0x01, 0x03);
var (
	localPrefix    byte = 0x01
	localPrefixKey      = []byte{localPrefix}
)

// containerKey
var (
	containerKey = []byte{localPrefix, 0x01}
)

// resources is in (0x01 0x02, 0x01 0x03)
var (
	resourcesPrefix = []byte{localPrefix, 0x02}
)

func getResourceKey(id uint64) []byte {
	n := len(resourcesPrefix) + 8
	buf := make([]byte, n, n)
	copy(buf[0:len(resourcesPrefix)], resourcesPrefix)
	binary.BigEndian.PutUint64(buf[len(resourcesPrefix):], id)
	return buf
}

// LocalStorage is the local data storage
type LocalStorage interface {
	// Get returns the key value
	Get(key []byte) ([]byte, error)
	// Set sets the key value to the local storage
	Set(pairs ...[]byte) error
	// Remove remove the key from the local storage
	Remove(keys ...[]byte) error
	// Range visit all values that start with prefix, set limit to 0 for no limit
	Range(prefix []byte, limit uint64, fn func(key, value []byte) bool) error
}

type localDB interface {
	get([]byte) ([]byte, error)
	set(...[]byte) error

	countResources() (int, error)
	loadResources(func(value []byte) (uint64, error)) error
	putResource(...Resource) error
	removeResource(...uint64) error
}

type defaultLocalDB struct {
	storage LocalStorage
}

func newLocalDB(storage LocalStorage) localDB {
	return &defaultLocalDB{
		storage: storage,
	}
}

func (db *defaultLocalDB) loadResources(handleFunc func(value []byte) (uint64, error)) error {
	return db.storage.Range(resourcesPrefix, 0, func(key, value []byte) bool {
		handleFunc(value)
		return true
	})
}

func (db *defaultLocalDB) countResources() (int, error) {
	c := 0
	err := db.storage.Range(resourcesPrefix, 0, func(key, value []byte) bool {
		c++
		return true
	})
	if err != nil {
		return 0, err
	}

	return c, nil
}

func (db *defaultLocalDB) putResource(reses ...Resource) error {
	var pairs [][]byte

	for _, res := range reses {
		data, err := res.Marshal()
		if err != nil {
			return err
		}

		pairs = append(pairs, getResourceKey(res.ID()), data)
	}

	return db.set(pairs...)
}

func (db *defaultLocalDB) removeResource(resIDs ...uint64) error {
	var keys [][]byte
	for _, resID := range resIDs {
		keys = append(keys, getResourceKey(resID))
	}

	return db.storage.Remove(keys...)
}

func (db *defaultLocalDB) set(pairs ...[]byte) error {
	return db.storage.Set(pairs...)
}

func (db *defaultLocalDB) get(key []byte) ([]byte, error) {
	return db.storage.Get(key)
}

// LocalStore the local data store
type LocalStore interface {
	// BootstrapCluster bootstrap the cluster,
	BootstrapCluster(initResources ...Resource)

	// MustPutResource put the resource to local
	MustPutResource(...Resource)

	// MustRemoveResource remove the res from the local
	MustRemoveResource(...uint64)

	// MustAllocID returns the new id by pd
	MustAllocID() uint64

	// MustCountResources returns local resources count
	MustCountResources() int

	// MustLoadResources load all local resources
	MustLoadResources(func(value []byte) (uint64, error))
}

// NewLocalStore returns a local store
func NewLocalStore(meta Container, storage LocalStorage, pd Prophet) LocalStore {
	return &defaultLocalStore{
		meta: meta,
		db:   &defaultLocalDB{storage: storage},
		pd:   pd,
	}
}

type defaultLocalStore struct {
	meta Container
	db   localDB
	pd   Prophet
}

func (ls *defaultLocalStore) BootstrapCluster(initResources ...Resource) {
	if len(initResources) == 0 {
		log.Warningf("init with empty resources")
	}

	data, err := ls.db.get(containerKey)
	if err != nil {
		log.Fatalf("load local container meta failed with %+v", err)
	}

	if len(data) > 0 {
		id := binary.BigEndian.Uint64(data)
		if id > 0 {
			ls.meta.SetID(id)
			log.Infof("load from local, container is %d", id)
			return
		}
	}

	id := ls.MustAllocID()
	ls.meta.SetID(id)
	log.Infof("init local container with id: %d", id)

	count, err := ls.db.countResources()
	if err != nil {
		log.Fatalf("bootstrap store failed with %+v", err)
	}
	if count > 0 {
		log.Fatal("local container is not empty and has already had data")
	}

	data = make([]byte, 8, 8)
	binary.BigEndian.PutUint64(data, id)
	err = ls.db.set(containerKey, data)
	if err != nil {
		log.Fatal("save local container id failed with %+v", err)
	}

	ok, err := ls.pd.GetStore().AlreadyBootstrapped()
	if err != nil {
		log.Fatal("get cluster already bootstrapped failed with %+v", err)
	}

	if !ok {
		// prepare init resource, alloc the resource id and the first replica peer info
		for _, res := range initResources {
			res.SetID(ls.MustAllocID())
			p := ls.newPeer()
			res.SetPeers([]*Peer{&p})
			ls.MustPutResource(res)
		}

		ok, err := ls.pd.GetStore().PutBootstrapped(ls.meta, initResources...)
		if err != nil {
			for _, res := range initResources {
				ls.MustRemoveResource(res.ID())
			}

			if err != errMaybeNotLeader {
				log.Fatal("bootstrap cluster failed with %+v", err)
			}

			log.Warningf("bootstrap cluster failed with %+v", err)
		}
		if !ok {
			log.Info("the cluster is already bootstrapped")
			for _, res := range initResources {
				ls.MustRemoveResource(res.ID())
			}
			log.Info("the init resources is already removed from container")
		}
	}

	ls.pd.GetRPC().TiggerContainerHeartbeat()
}

func (ls *defaultLocalStore) MustPutResource(res ...Resource) {
	err := ls.db.putResource(res...)
	if err != nil {
		log.Fatalf("save resource %+v failed with %+v",
			res,
			err)
	}
}

func (ls *defaultLocalStore) MustRemoveResource(resIDs ...uint64) {
	err := ls.db.removeResource(resIDs...)
	if err != nil {
		log.Fatalf("remove resource %d failed with %+v",
			resIDs,
			err)
	}
}

func (ls *defaultLocalStore) MustAllocID() uint64 {
	id, err := ls.pd.GetRPC().AllocID()
	if err != nil {
		log.Fatalf("alloc id failed with %+v", err)
	}
	return id
}

func (ls *defaultLocalStore) MustCountResources() int {
	c, err := ls.db.countResources()
	if err != nil {
		log.Fatalf("get fetch local resources count failed with %+v", err)
	}

	return c
}

func (ls *defaultLocalStore) MustLoadResources(handleFunc func(value []byte) (uint64, error)) {
	err := ls.db.loadResources(handleFunc)
	if err != nil {
		log.Fatalf("get load local resources failed with %+v", err)
	}
}

func (ls *defaultLocalStore) newPeer() Peer {
	return Peer{
		ID:          ls.MustAllocID(),
		ContainerID: ls.meta.ID(),
	}
}

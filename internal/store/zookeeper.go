// Copyright 2017 Sorint.lab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const zookeeperErrorPrefix = "zookeeperstore"
const keySeparator = "/"

type zookeeperStore struct {
	c              *zk.Conn
	requestTimeout time.Duration
}

func newZookeeperStore(addrs []string, storeTimeout time.Duration) (*zookeeperStore, error) {
	c, _, err := zk.Connect(addrs, storeTimeout)
	if err != nil {
		return nil, fmt.Errorf("%s: Error while connecting to zookeeper: %v, %v", zookeeperErrorPrefix, addrs, err)
	}

	return &zookeeperStore{c: c, requestTimeout: storeTimeout}, nil
}

func (s *zookeeperStore) createKeyRecursively(key string, flags int32, acl []zk.ACL) error {
	directories := strings.Split(key, "/")

	// i==0 is Empty string as key should begin with "/"
	incrementalPath := "/"
	for i := 1; i < len(directories); i++ {
		incrementalPath = path.Join(incrementalPath, directories[i])
		exists, _, err := s.c.Exists(incrementalPath)

		if err != nil {
			return fmt.Errorf("%s: Exists failed while checking for incrementalPath: %s %v", zookeeperErrorPrefix, incrementalPath, err)
		}

		if !exists {
			_, err = s.c.Create(incrementalPath, []byte{}, flags, acl)
			if err != nil {
				return fmt.Errorf("%s: Error when creating key: %s %v", zookeeperErrorPrefix, incrementalPath, err)
			}
		}
	}

	return nil
}

func normalizeKey(key string) string {
	normalizedKey := key
	if !strings.HasPrefix(key, keySeparator) {
		normalizedKey = fmt.Sprintf("%s%s", keySeparator, key)
	}

	return strings.TrimSuffix(normalizedKey, keySeparator)
}

func (s *zookeeperStore) Put(_ context.Context, key string, value []byte, options *WriteOptions) error {
	key = normalizeKey(key)
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	s.createKeyRecursively(key, flags, acl)

	_, stat, err := s.c.Exists(key)
	if err != nil {
		return fmt.Errorf("%s: Error checking if key exists: %s, %v", zookeeperErrorPrefix, key, err)
	}

	_, err = s.c.Set(key, value, stat.Version)

	if err != nil {
		return fmt.Errorf("%s: Set key failed: %s, %v", zookeeperErrorPrefix, key, err)
	}

	return nil

}

func (s *zookeeperStore) Get(_ context.Context, key string) (*KVPair, error) {
	key = normalizeKey(key)
	value, stat, err := s.c.Get(key)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("%s: Get key failed: %s, %v", zookeeperErrorPrefix, key, err)
	}

	return &KVPair{Key: key, Value: value, LastIndex: uint64(stat.Version)}, nil
}

func (s *zookeeperStore) List(ctx context.Context, directory string) ([]*KVPair, error) {
	directory = normalizeKey(directory)
	keys, _, err := s.c.Children(directory)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	kv := []*KVPair{}

	for _, key := range keys {
		pair, err := s.Get(ctx, strings.TrimSuffix(directory, "/")+key)
		if err != nil {
			return nil, err
		}

		kv = append(kv, pair)
	}

	return kv, nil
}

func (s *zookeeperStore) AtomicPut(_ context.Context, key string, value []byte, previous *KVPair, options *WriteOptions) (*KVPair, error) {
	key = normalizeKey(key)
	var lastIndex uint64
	if previous != nil {
		stat, err := s.c.Set(key, value, int32(previous.LastIndex))
		if err != nil {
			if err == zk.ErrBadVersion {
				return nil, ErrKeyModified
			}

			return nil, fmt.Errorf("%s: Error AtomicPut of key %s, %v", zookeeperErrorPrefix, key, err)
		}

		lastIndex = uint64(stat.Version)
	} else {
		flags := int32(0)
		acls := zk.WorldACL(zk.PermAll)
		// Skip first (empty string)
		dirs := strings.Split(key, "/")[1:]
		// Skip last as that is the target key to be created
		intermediaryPaths := ""
		for i := 1; i < len(dirs)-1; i++ {
			intermediaryPaths = path.Join(intermediaryPaths, dirs[i])
		}

		err := s.createKeyRecursively(intermediaryPaths, flags, acls)
		if err != nil {
			return nil, fmt.Errorf("%s: Error creating parent in AtomicPut: %s, %v", zookeeperErrorPrefix, intermediaryPaths, err)
		}

		_, err = s.c.Create(key, value, flags, acls)
		if err != nil {
			return nil, fmt.Errorf("%s: Error creating key in AtomicPut: %s, %v", zookeeperErrorPrefix, key, err)
		}

		lastIndex = 0
	}
	return &KVPair{Key: key, Value: value, LastIndex: lastIndex}, nil
}

func (s *zookeeperStore) Delete(_ context.Context, key string) error {
	key = normalizeKey(key)
	err := s.c.Delete(key, -1)
	if err != nil {
		return fmt.Errorf("%s: Delete key failed: %s, %v", zookeeperErrorPrefix, key, err)
	}

	return nil
}

func (s *zookeeperStore) Close() error {
	s.c.Close()
	return nil
}

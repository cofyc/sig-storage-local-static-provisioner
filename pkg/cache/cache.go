/*
Copyright 2017 The Kubernetes Authors.

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

package cache

import (
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"

	v1 "k8s.io/api/core/v1"
)

// VolumeCache keeps all the PersistentVolumes that have been created by this provisioner.
// It is periodically updated by the Populator.
// The Deleter and Discoverer use the VolumeCache to check on created PVs
type VolumeCache struct {
	pvStore    cache.Store
	filterFunc func(obj interface{}) bool
	keyFunc    cache.KeyFunc
}

// NewVolumeCache creates a new PV cache object for storing PVs created by this provisioner.
func NewVolumeCache(filterFunc func(obj interface{}) bool) *VolumeCache {
	keyFunc := cache.DeletionHandlingMetaNamespaceKeyFunc
	return &VolumeCache{
		pvStore:    cache.NewStore(keyFunc),
		filterFunc: filterFunc,
		keyFunc:    keyFunc,
	}
}

// GetPV returns the PV object given the PV name
func (cache *VolumeCache) GetPV(pvName string) (*v1.PersistentVolume, bool) {
	obj, exists, _ := cache.pvStore.GetByKey(pvName)
	var pv *v1.PersistentVolume
	if exists {
		pv = obj.(*v1.PersistentVolume)
	}
	return pv, exists
}

// AddPV adds the PV object to the cache
func (cache *VolumeCache) AddPV(pv *v1.PersistentVolume) {
	cache.pvStore.Add(pv)
	klog.Infof("Added pv %q to cache", pv.Name)
}

// UpdatePV updates the PV object in the cache
func (cache *VolumeCache) UpdatePV(pv *v1.PersistentVolume) {
	cache.pvStore.Update(pv)
	klog.Infof("Updated pv %q to cache", pv.Name)
}

// DeletePV deletes the PV object from the cache
func (cache *VolumeCache) DeletePV(pv *v1.PersistentVolume) {
	cache.pvStore.Delete(pv)
	klog.Infof("Deleted pv %q from cache", pv.Name)
}

// ListPVs returns a list of all the PVs in the cache
func (cache *VolumeCache) ListPVs() []*v1.PersistentVolume {
	pvs := []*v1.PersistentVolume{}
	objs := cache.pvStore.List()
	for _, obj := range objs {
		pvs = append(pvs, obj.(*v1.PersistentVolume))
	}
	return pvs
}

func (cache *VolumeCache) exists(obj interface{}) bool {
	key, err := cache.keyFunc(obj)
	if err != nil {
		return false
	}
	_, exists, err := cache.pvStore.GetByKey(key)
	if err != nil {
		return false
	}
	return exists
}

func (cache *VolumeCache) add(obj interface{}) error {
	if cache.exists(obj) || cache.filterFunc(obj) {
		return cache.pvStore.Add(obj)
	}
	return nil
}

func (cache *VolumeCache) Add(obj interface{}) error {
	return cache.add(obj)
}

func (cache *VolumeCache) Update(obj interface{}) error {
	return cache.add(obj)
}

func (cache *VolumeCache) Delete(obj interface{}) error {
	return cache.pvStore.Delete(obj)
}

func (cache *VolumeCache) Get(obj interface{}) (item interface{}, exists bool, err error) {
	return cache.pvStore.Get(obj)
}

func (cache *VolumeCache) GetByKey(key string) (item interface{}, exists bool, err error) {
	return cache.pvStore.GetByKey(key)
}

func (cache *VolumeCache) List() []interface{} {
	return cache.pvStore.List()
}

func (cache *VolumeCache) ListKeys() []string {
	return cache.pvStore.ListKeys()
}

func (cache *VolumeCache) Replace(objs []interface{}, resourceVersion string) error {
	pvObjs := make([]interface{}, 0)
	for _, obj := range objs {
		if cache.exists(obj) || cache.filterFunc(obj) {
			pvObjs = append(pvObjs, obj)
		}
	}
	return cache.pvStore.Replace(pvObjs, resourceVersion)
}

func (cache *VolumeCache) Resync() error {
	return cache.pvStore.Resync()
}

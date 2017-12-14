package snowflake

import (
	"sync"
	"testing"
)

type IdMap struct {
	idmaps  map[int64]map[int64]bool
	genmaps map[int64]*SnowFlake
	sync.Mutex
}

var idMaps *IdMap

func init() {
	idMaps = &IdMap{
		idmaps:  map[int64]map[int64]bool{},
		genmaps: map[int64]*SnowFlake{},
	}
	for i := 0; i < 1024; i++ {
		workerid := int64(i)
		idMaps.idmaps[workerid] = map[int64]bool{}
		idMaps.genmaps[workerid] = DefaultSnowFlake(workerid)
	}
}

func TestAll(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1024)
	for i := 0; i < 1024; i++ {
		workerid := int64(i)
		go func() {
			defer wg.Done()
			for i := 0; i < 1e3; i++ {
				idMaps.Lock()
				gen := idMaps.genmaps[workerid]
				id, err := gen.GenId()
				if err != nil {
					t.Fatalf("error genId:%s\n", err.Error())
				}
				if _, ok := idMaps.idmaps[workerid][id]; ok {
					t.Fatalf("repeated id:%d\n", id)
				}
				idMaps.idmaps[workerid][id] = true
				idMaps.Unlock()
			}
		}()
	}
	wg.Wait()
}

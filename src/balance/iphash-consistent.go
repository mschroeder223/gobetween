/**
 * iphash-consistent.go - consistent iphash balance impl
 *
 * @author Illarion Kovalchuk <illarion.kovalchuk@gmail.com>
 */

package balance

import (
	"../core"
	"errors"
	"hash/fnv"
)

/**
 * Iphash balancer
 */
type IphashConsistentBalancer struct {
}

/**
 * Elect backend using consistent iphash strategy. This is naive implementation
 * using Key+Node Hash Algorithm for stable sharding described at http://kennethxu.blogspot.com/2012/11/sharding-algorithm.html
 *
 * TODO: Improve as needed
 */
func (b *IphashConsistentBalancer) Elect(context core.Context, backends []*core.Backend) (*core.Backend, error) {

	if len(backends) == 0 {
		return nil, errors.New("Can't elect backend, Backends empty")
	}

	var result *core.Backend
	{
		var bestHash uint32

		for i, backend := range backends {
			hasher := fnv.New32a()
			hasher.Write(context.Ip())
			hasher.Write([]byte(backend.Address()))
			s32 := hasher.Sum32()
			if s32 > bestHash {
				bestHash = s32
				result = backends[i]
			}
		}
	}

	return result, nil
}

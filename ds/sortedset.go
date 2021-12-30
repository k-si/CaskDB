package ds

type SortedSet struct {
	record map[string]*zSet
}

type zSet struct {
	sl   *SkipList
	dict map[string]*skipListNode
}

//type pair struct {
//	member string
//	score  float64
//}
//
//func (p *pair) GetPair() (string, float64) {
//	if p != nil {
//		return p.member, p.score
//	}
//	return "", 0
//}

func NewSortedSet() *SortedSet {
	return &SortedSet{record: make(map[string]*zSet)}
}

func newZSet() *zSet {
	return &zSet{
		sl:   NewSkipList(),
		dict: make(map[string]*skipListNode),
	}
}

func (ss *SortedSet) Add(key, member string, score float64) {
	if !ss.KeyExist(key) {
		ss.record[key] = newZSet()
	}

	zset := ss.record[key]
	n, exist := zset.dict[member]

	if exist {
		if n.score == score {
			return
		}
		zset.sl.Delete(n.score, member)
	}
	node := zset.sl.Insert(score, member)
	zset.dict[member] = node
}

func (ss *SortedSet) Remove(key, member string) {
	if ss.MemberExist(key, member) {
		n := ss.record[key].dict[member]
		ss.record[key].sl.Delete(n.score, member)
		delete(ss.record[key].dict, member)
	}
}

func (ss *SortedSet) Top(key string, n int) (res []interface{}) {
	if ss.KeyExist(key) {
		zset := ss.record[key]
		for p, i := zset.sl.head, 0; i < n && p != nil; p = p.next[0] {
			if p != zset.sl.head {
				res = append(res, p.member, p.score)
			}
		}
	}
	return
}

func (ss *SortedSet) RangeByScore(key string, start, stop float64) (res []interface{}) {
	if start > stop {
		return
	}
	if ss.KeyExist(key) {
		zset := ss.record[key]
		st := zset.sl.Find(start, GE)
		sp := zset.sl.Find(stop, LE)
		if st == nil || sp == nil {
			return
		}

		for p := st; p != nil; p = p.next[0] {
			res = append(res, p.member, p.score)
			if p == sp {
				break
			}
		}
	}
	return
}

func (ss *SortedSet) GetScore(key, member string) (bool, float64) {
	if ss.MemberExist(key, member) {
		return true, ss.record[key].dict[member].score
	}
	return false, 0
}

func (ss *SortedSet) GetCard(key string) int {
	if ss.KeyExist(key) {
		return len(ss.record[key].dict)
	}
	return 0
}

func (ss *SortedSet) KeyExist(key string) bool {
	_, exist := ss.record[key]
	return exist
}

func (ss *SortedSet) MemberExist(key, member string) bool {
	if ss.KeyExist(key) {
		_, exist := ss.record[key].dict[member]
		return exist
	}
	return false
}

func correctRange(start, stop int, length int) (int, int, bool) {
	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += length
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return 0, 0, false
	}
	return start, stop, true
}

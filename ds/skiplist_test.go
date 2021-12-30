package ds

import "testing"

func TestSkipList_Insert_Delete(t *testing.T) {
	sl := NewSkipList()
	sl.Delete(0, "zhang san")
	sl.Insert(10, "li si")
	sl.Insert(10, "zhang san")
	sl.Insert(11, "li si")
	sl.Delete(10, "li si")
}

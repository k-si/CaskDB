package ds

import (
	"bytes"
)

var isRem = false

type AVLTree struct {
	root *aVLTreeNode
	size int
}

func NewAVLTree() *AVLTree {
	return &AVLTree{}
}

func (t *AVLTree) Put(key []byte, value interface{}) {
	t.root = insert(t.root, key, value)
	t.size++
}

func (t *AVLTree) Remove(key []byte) {
	t.root = remove(t.root, key)
	if isRem {
		t.size--
		isRem = false
	}
}

func (t *AVLTree) Get(key []byte) interface{} {
	if n := find(t.root, key); n != nil {
		return n.value
	}
	return nil
}

func (t *AVLTree) Size() int {
	return t.size
}

type aVLTreeNode struct {
	key    []byte
	value  interface{}
	height int
	left   *aVLTreeNode
	right  *aVLTreeNode
}

func newAVLNode(key []byte, value interface{}, left, right *aVLTreeNode) *aVLTreeNode {
	return &aVLTreeNode{
		key:    key,
		value:  value,
		height: 1,
		left:   left,
		right:  right,
	}
}

// 插入节点
func insert(cur *aVLTreeNode, key []byte, value interface{}) *aVLTreeNode {
	if cur == nil {
		cur = newAVLNode(key, value, nil, nil)
		return cur
	}

	if bytes.Compare(cur.key, key) == 0 {
		cur.value = value
	} else if bytes.Compare(key, cur.key) > 0 {
		cur.right = insert(cur.right, key, value)
		if getHeight(cur.right)-getHeight(cur.left) == 2 {
			if bytes.Compare(key, cur.right.key) > 0 {
				cur = leftRotation(cur) // 情况1：需要单左旋
			} else {
				cur = rightLeftRotation(cur) // 情况2：需要右旋再左旋
			}
		}
	} else {
		cur.left = insert(cur.left, key, value)
		if getHeight(cur.left)-getHeight(cur.right) == 2 {
			if bytes.Compare(key, cur.left.key) < 0 {
				cur = rightRotation(cur) // 情况3：需要单右旋
			} else {
				cur = leftRightRotation(cur) // 情况4：需要左旋再右旋
			}
		}
	}

	cur.height = max(getHeight(cur.left), getHeight(cur.right)) + 1

	return cur
}

// 删除节点
func remove(cur *aVLTreeNode, key []byte) *aVLTreeNode {
	if cur == nil {
		return nil
	}

	if bytes.Compare(cur.key, key) == 0 {
		isRem = true

		// 存在左右子树
		if cur.left != nil && cur.right != nil {

			// 左子树高，通过赋值删除左子树最大值节点，维护树的有序
			if getHeight(cur.left) > getHeight(cur.right) {
				m := getMax(cur.left)
				cur.key, cur.value = m.key, m.value
				cur.left = remove(cur.left, m.key)
			} else {
				// 右子树高，删右子树最小值节点
				m := getMin(cur.right)
				cur.key, cur.value = m.key, m.value
				cur.right = remove(cur.right, m.key)
			}
		} else {
			// 只有一个子树或无子树
			if cur.left != nil {
				cur = cur.left
			} else if cur.right != nil {
				cur = cur.right
			} else {
				cur = nil
			}
		}
	} else if bytes.Compare(key, cur.key) > 0 {

		// 删除右子树节点，相当于在左子树插入节点
		cur.right = remove(cur.right, key)

		// 失衡调整
		if getHeight(cur.left)-getHeight(cur.right) == 2 {
			if getHeight(cur.left.right) > getHeight(cur.left.left) {
				cur = leftRightRotation(cur)
			} else {
				cur = rightRotation(cur) // 相当于情况3、4
			}
		} else {
			cur.height = max(getHeight(cur.left), getHeight(cur.right)) + 1
		}
	} else {

		// 删除左子树节点，相当于在右子树插入节点
		cur.left = remove(cur.left, key)

		// 失衡调整
		if getHeight(cur.right)-getHeight(cur.left) == 2 {
			if getHeight(cur.right.left) > getHeight(cur.right.right) {
				cur = rightLeftRotation(cur)
			} else {
				cur = leftRotation(cur) // 相当于情况1、2
			}
		} else {
			cur.height = max(getHeight(cur.left), getHeight(cur.right)) + 1
		}
	}

	return cur
}

// 查找节点
func find(cur *aVLTreeNode, key []byte) *aVLTreeNode {
	if cur == nil {
		return nil
	}
	if bytes.Compare(key, cur.key) == 0 {
		return cur
	} else if bytes.Compare(key, cur.key) > 0 {
		return find(cur.right, key)
	} else {
		return find(cur.left, key)
	}
}

// 单左旋操作
// 4              5
//  \            / \
//   5    ->    4   6
//    \
//     6
// 参数cur为最小失衡子树的根节点，在图中为节点4
// 若节点5有左子树，则该左子树成为节点4的右子树
// 节点4成为节点5的左子树
// 最后更新节点的高度值
func leftRotation(n *aVLTreeNode) *aVLTreeNode {
	cur := n.right
	n.right = cur.left
	cur.left = n

	n.height = max(getHeight(n.left), getHeight(n.right)) + 1
	cur.height = max(getHeight(cur.left), getHeight(cur.right)) + 1

	return cur
}

// 镜像的单右旋操作
func rightRotation(n *aVLTreeNode) *aVLTreeNode {
	cur := n.left
	n.left = cur.right
	cur.right = n

	n.height = max(getHeight(n.left), getHeight(n.right)) + 1
	cur.height = max(getHeight(cur.left), getHeight(cur.right)) + 1

	return cur
}

// 先右旋再左旋
func rightLeftRotation(n *aVLTreeNode) *aVLTreeNode {
	n.right = rightRotation(n.right)
	return leftRotation(n)
}

// 先左旋再右旋
//        5                   5                    5
//       / \                 / \                  / \
//      3   7               3   7                3   7
//     / \ / \             / \ / \              / \ / \
//    2  4 6  8    ->     2  4 6  8     ->     1  4 6  8
//   /                   /                    / \
//  0                   1                    0   2
//   \                 /
//    1               0
// 图中2为最小失衡子树的根节点
// 将2的右左子树进行左旋
// 再将以2为根节点的子树右旋
func leftRightRotation(n *aVLTreeNode) *aVLTreeNode {
	n.left = leftRotation(n.left)
	return rightRotation(n)
}

// 获取节点高度
func getHeight(n *aVLTreeNode) int {
	if n == nil {
		return 0
	}
	return n.height
	//return max(getHeight(n.left), getHeight(n.right)) + 1
}

// 子树中最大值节点
func getMax(n *aVLTreeNode) *aVLTreeNode {
	if n == nil {
		return nil
	}
	for n.right != nil {
		n = n.right
	}
	return n
}

// 子树中最小值节点
func getMin(n *aVLTreeNode) *aVLTreeNode {
	if n == nil {
		return nil
	}
	for n.left != nil {
		n = n.left
	}
	return n
}

func max(v1, v2 int) int {
	if v1 > v2 {
		return v1
	} else {
		return v2
	}
}

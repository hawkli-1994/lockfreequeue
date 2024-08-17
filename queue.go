package lockfreequeue

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type Queue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
	len  uint64
	pool sync.Pool
}

// NewQueue 创建并返回一个新的队列实例。
// 该函数通过初始化队列的头部和尾部指针，并设置队列长度为0，以及配置一个用于回收directItem的同步池。
// 返回值:
//
//	*Queue - 一个指向新创建的队列的指针。
func NewQueue() *Queue {
	// 初始化队列的头部，它是一个特殊的directItem，其next指向第一个有效元素，v为nil表示头部不存储值。
	head := directItem{
		next: nil,
		v:    nil,
	}
	// 返回新的队列实例
	return &Queue{
		head: unsafe.Pointer(&head), // 设置头部指针
		tail: unsafe.Pointer(&head), // 设置尾部指针，初始时与头部相同
		len:  0,                     // 初始队列长度为0
		pool: sync.Pool{ // 初始化同步池，用于directItem的回收
			New: func() any {
				return &directItem{} // 池的New方法，用于生成新的directItem实例
			},
		},
	}
}

// Enqueue 将一个元素添加到队列的末尾。
// 该操作是线程安全的，确保多个goroutine同时添加元素时队列的完整性。
// 参数:
//
//	v: 要添加到队列的元素，可以是任何类型的值。
func (q *Queue) Enqueue(v any) {
	// 从共享池中获取一个directItem，并初始化它。
	// 这样做既减少了内存分配的开销，也统一了队列元素的管理。
	i := q.pool.Get().(*directItem)
	i.next = nil
	i.v = v

	// 初始化last和lastNext指针，用于在循环中追踪队列的尾部。
	var last, lastNext *directItem

	// 使用CAS操作循环尝试更新队列的尾部。
	// 这个循环确保了在多线程环境下队列的尾部能够正确更新。
	for {
		// 加载当前队列的尾部指针。
		last = loaditem(&q.tail)
		// 加载当前尾部指针的下一个元素。
		lastNext = loaditem(&last.next)

		// 再次检查队列的尾部指针是否未变，
		// 这是必要的，因为在上一次加载后，可能已经被其他goroutine修改。
		if loaditem(&q.tail) == last {
			// 如果当前尾部的下一个元素为空，说明可以将新元素添加到队列的末尾。
			if lastNext == nil {
				// 使用CAS操作更新尾部的下一个元素为新元素，并更新队列的尾部指针。
				// 这样做保证了更新操作的原子性，避免了竞态条件。
				if casitem(&last.next, lastNext, i) {
					// 更新队列的尾部指针，确保队列的尾部正确指向新的元素。
					casitem(&q.tail, last, i)
					// 原子性增加队列的长度。
					atomic.AddUint64(&q.len, 1)
					// 添加成功，退出函数。
					return
				}
			} else {
				// 如果当前尾部的下一个元素不为空，说明有其他goroutine已经添加了元素，
				// 或者正在尝试添加。此时需要更新队列的尾部指针，以避免死锁。
				// 这个操作确保了队列的持续可操作性，即使在高并发环境下。
				casitem(&q.tail, last, lastNext)
			}
		}
	}
}

// Dequeue 从队列中移除并返回一个元素。
// 如果队列为空，函数返回 nil。
// Dequeue 从队列中移除并返回一个元素。这个操作是线程安全的。
// 如果队列为空，函数返回 nil。
func (q *Queue) Dequeue() interface{} {
	// 定义指向队列首尾和首元素下一个元素的指针
	var first, last, firstnext *directItem
	for {
		// 读取队列头部和尾部的元素
		first = loaditem(&q.head)
		last = loaditem(&q.tail)
		// 读取队列头部元素的下一个元素
		firstnext = loaditem(&first.next)
		// 检查队列的头部、尾部和下一个元素是否一致
		if first == loaditem(&q.head) {
			// 检查队列是否为空
			if first == last {
				// 如果队列确实为空
				if firstnext == nil {
					// 队列为空，无法移除元素，返回 nil
					return nil
				}
				// 尾部指针落后，尝试将其向前移动
				casitem(&q.tail, last, firstnext)
			} else {
				// 在尝试交换头部指针之前读取值，否则另一个移除操作可能会释放下一个节点
				v := firstnext.v
				// 尝试将头部指针移动到下一个节点
				if casitem(&q.head, first, firstnext) {
					// 队列长度减一
					atomic.AddUint64(&q.len, ^uint64(0))
					// 回收被移除的元素
					q.pool.Put(first)
					// 返回移除的元素
					return v
				}
			}
		}
	}
}

// Length returns the length of the queue.
func (q *Queue) Length() uint64 {
	return atomic.LoadUint64(&q.len)
}

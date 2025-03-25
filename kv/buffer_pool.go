package kv

import (
	"container/list"
	"fmt"
)

type BufferPool struct {
	pool map[int]*list.Element
	queue *list.List
	capacity int
}

type BufferPoolEntry struct {
	page *Page
	dirty bool // whether the page has been modified
	refCount int
}

func NewBufferPool(capacity int) *BufferPool {
	return &BufferPool{
		pool:     make(map[int]*list.Element),
		queue:    list.New(),
		capacity: capacity,
	}
}
func (bp *BufferPool) GetPage(id int) (*Page, error) {
	if entry, ok := bp.pool[id]; ok {
		bp.queue.MoveToFront(entry)
		entry.Value.(*BufferPoolEntry).refCount++
		return entry.Value.(*BufferPoolEntry).page, nil
	}

	return nil, fmt.Errorf("page not found")	
}

func (bp *BufferPool) PutPage(page *Page) error {
	if entry, ok := bp.pool[page.ID]; ok {
		bp.queue.MoveToFront(entry)
		entry.Value.(*BufferPoolEntry).refCount++
		return nil
	}

	if bp.queue.Len() >= bp.capacity {
		bp.evictPage()
	}

	bp.pool[page.ID] = bp.queue.PushFront(&BufferPoolEntry{page: page, dirty: false, refCount: 1})
	return nil
}

func (bp *BufferPool) evictPage() {
	for bp.queue.Len() >= bp.capacity {
		entry := bp.queue.Back()
		bp.queue.Remove(entry)
	}
}

func (bp *BufferPool) PrintBufferPool() {
	fmt.Println("Buffer Pool:")
	for _, entry := range bp.pool {
		fmt.Printf("Page ID: %d, Dirty: %t, Ref Count: %d\n", entry.Value.(*BufferPoolEntry).page.ID, entry.Value.(*BufferPoolEntry).dirty, entry.Value.(*BufferPoolEntry).refCount)
	}
}

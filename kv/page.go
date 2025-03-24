package kv

import "fmt"

type Page struct {
	ID int
	Data []byte
}


func NewPage(id int, pageSize int) *Page {
	return &Page{
		ID: id,
		Data: make([]byte, pageSize),
	}
}

func (p *Page) WriteData(offset int, data []byte) error {
	if offset+len(data) > len(p.Data) {
		return fmt.Errorf("data is too large to write to page")
	}

	copy(p.Data[offset:], data)
	return nil
}

func (p *Page) ReadData(offset int, length int) ([]byte, error) {
	if offset+length > len(p.Data) {
		return nil, fmt.Errorf("read out of bounds")
	}
	return p.Data[offset:offset+length], nil
}

func (p *Page) PrintPage() {
	fmt.Printf("Page ID: %d\n", p.ID)
	fmt.Printf("Page Data: %v\n", p.Data)
}

func (p *Page) Serialize() []byte {
	return p.Data
}

func Deserialize(pageData []byte, id int, pageSize int) (*Page, error) {
	if len(pageData) != pageSize {
		return nil, fmt.Errorf("page data length does not match page size")
	}

	return &Page{
		ID:   id,
		Data: pageData,
	}, nil
}

package dispatcher

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/puper/go-jsonrpc/jsonrpc"
)

func uint64ToByte(n uint64) []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, n)
	return buf.Bytes()
}

func byteToUint64(v []byte) (n uint64) {
	buf := bytes.NewBuffer(v)
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func WriteUint64(buf *bytes.Buffer, n uint64) {
	binary.Write(buf, binary.BigEndian, n)
}

func ReadUint64(buf *bytes.Buffer) (n uint64) {
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func WriteUint8(buf *bytes.Buffer, n uint8) {
	binary.Write(buf, binary.BigEndian, n)
}

func ReadUint8(buf *bytes.Buffer) (n uint8) {
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func WriteFloat64(buf *bytes.Buffer, n float64) {
	binary.Write(buf, binary.BigEndian, n)
}

func ReadFloat64(buf *bytes.Buffer) (n float64) {
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func WriteBytes(buf *bytes.Buffer, v []byte) {
	l := uint64(len(v))
	WriteUint64(buf, l)
	buf.Write(v)
}

func ReadBytes(buf *bytes.Buffer) (v []byte) {
	l := ReadUint64(buf)
	return buf.Next(int(l))
}

func WriteString(buf *bytes.Buffer, v string) {
	WriteBytes(buf, []byte(v))
}

func ReadString(buf *bytes.Buffer) (v string) {
	return string(ReadBytes(buf))
}

type Job struct {
	Id      uint64
	Data    string
	Type    string
	Key     string
	Storage *Storage
	Rule    *Rule
}

func (this *Job) execute() error {
	if this.Rule.HandlerType == "jsonrpc" {
		client := jsonrpc.NewClient(this.Rule.HandlerUrl)
		params := make(map[string]string)
		params["data"] = this.Data
		resp, err := client.CallTimeout(this.Rule.HandlerName, params, time.Duration(this.Rule.Timeout)*time.Second)
		if err == nil && resp.Error == nil {
			return nil
		}
		if err != nil {
			return err
		}
		if resp.Error != nil {
			return resp.Error
		}
	}
	return nil
}

func (this *Job) Execute() {
	wg.Add(1)
	defer wg.Done()
	var i uint64 = 0
LOOP:
	for {
		i++
		err := this.execute()
		if err != nil {
			if this.Rule.TryCount > 0 && uint64(this.Rule.TryCount) <= i {
				break LOOP
			}
			if i > 10 {
				time.Sleep(time.Second * 60)
			} else if i > 2 {
				time.Sleep(time.Second)
			}
		} else {
			break LOOP
		}

	}
	this.Storage.Delete(this.Id)
}

func NewJob(id uint64, data []byte) *Job {
	m := new(Job)
	buf := bytes.NewBuffer(data)
	m.Id = id
	m.Data = ReadString(buf)
	m.Type = ReadString(buf)
	m.Key = ReadString(buf)
	return m
}

func (this *Job) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	WriteString(buf, this.Data)
	WriteString(buf, this.Type)
	WriteString(buf, this.Key)
	return buf.Bytes()
}

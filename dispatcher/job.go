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

func (this *Job) Execute() {
	wg.Add(1)
	defer wg.Done()
	if this.Rule.TryCount == 0 {
		for {
			i := 0
			if this.Rule.HandlerType == "jsonrpc" {
				client := jsonrpc.NewClient(this.Rule.HandlerUrl)
				params := make(map[string]string)
				params["data"] = this.Data
				resp, err := client.CallTimeout(this.Rule.HandlerName, params, time.Duration(this.Rule.Timeout)*time.Second)
				if err == nil && resp.Error == nil {
					break
				}
				if i > 10 {
					time.Sleep(time.Second * 60)
				} else if i > 0 {
					time.Sleep(time.Second)
				}
			}
		}
	} else {
		var i uint8 = 0
		for ; i < this.Rule.TryCount; i++ {
			if this.Rule.HandlerType == "jsonrpc" {
				client := jsonrpc.NewClient(this.Rule.HandlerUrl)
				params := make(map[string]string)
				params["data"] = this.Data
				resp, err := client.CallTimeout(this.Rule.HandlerName, params, time.Duration(this.Rule.Timeout)*time.Second)
				if err == nil && resp.Error == nil {
					break
				}
				if i > 0 {
					time.Sleep(time.Second)
				}
			}
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

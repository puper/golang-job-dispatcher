package dispatcher

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"
	"time"
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
	Rules   []Rule
}

func (this *Job) Execute() {
	time.Sleep(time.Second * time.Duration(rand.Intn(5)*2))
	log.Println(this)
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

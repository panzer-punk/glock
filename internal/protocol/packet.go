package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"
)

/*
------
Packet
------
HEADERS:
Version - 1 byte
Type - 1 byte
PayloadLength - 4 byte
PAYLOAD_BLOCK:
Type - 1 byte
Len - 2 byte
Value - variadic
*/
const (
	ProtoVersionHeaderSize        = 1
	PacketTypeHeaderSize          = 1
	PacketPayloadLengthHeaderSize = 4
	PacketHeaderSize              = ProtoVersionHeaderSize + PacketTypeHeaderSize + PacketPayloadLengthHeaderSize

	PayloadBlockTypeHeaderSize   = 1
	PayloadBlockLengthHeaderSize = 2
	PayloadBlockHeaderSize       = PayloadBlockTypeHeaderSize + PayloadBlockLengthHeaderSize
)

type PayloadBlockType uint8

const (
	PayloadBlockTypeNamespace = iota
	PayloadBlockTypeKey
	PayloadBlockTypeValue
	PayloadBlockTypeTTL
	PayloadBlockTypeSecret

	PayloadBlockTypeSuccess //success boolean
	PayloadBlockTypeError   //error message

	//size of blocks array in Packet
	blocksSize = PayloadBlockTypeError + 1
)

type PacketType uint8

const (
	//Lock operations
	PacketTypeLock = iota
	PacketTypeTryLock
	PacketTypeUnlock

	//Generic error and success responses
	PacketTypeError
	PacketTypeSuccess
)

var (
	ErrInvalidPacketLength  = errors.New("invalid packet length")
	ErrInvalidPacket        = errors.New("invalid packet")
	ErrInvalidPayloadLength = errors.New("invalid payload length")
)

type PayloadBlock struct {
	//Headers
	Type PayloadBlockType
	Len  uint16

	//Payload
	Value []byte
}

func (p *PayloadBlock) serialize(buf *bytes.Buffer) {
	var len [2]byte
	binary.BigEndian.PutUint16(len[:], p.Len)

	buf.WriteByte(byte(p.Type))
	buf.Write(len[:])
	buf.Write(p.Value)
}

func (p *PayloadBlock) Size() uint32 {
	return uint32(PayloadBlockHeaderSize + p.Len)
}

func (p *PayloadBlock) Empty() bool {
	return p.Len == 0 && len(p.Value) == 0
}

func NewPayloadBlock(tp PayloadBlockType, value []byte) PayloadBlock {
	return PayloadBlock{
		Type:  tp,
		Len:   uint16(len(value)),
		Value: value,
	}
}

type Packet struct {
	//Headers
	Version       uint8
	Type          PacketType
	PayloadLength uint32

	//Payload
	Blocks [blocksSize]PayloadBlock
}

const (
	ProtoVersion = 1
)

func NewPacket(tp PacketType) Packet {
	return Packet{
		Version:       ProtoVersion,
		Type:          tp,
		PayloadLength: 0,
	}
}

func NewErrPacket(err error) *Packet {
	packet := NewPacket(PacketTypeError)
	errMsg := err.Error()
	block := NewPayloadBlock(
		PayloadBlockTypeError,
		unsafe.Slice(unsafe.StringData(errMsg), len(errMsg)),
	)
	packet.AddBlock(block)

	return &packet
}

func (p *Packet) Serialize(buf *bytes.Buffer) {
	var headers [PacketHeaderSize]byte

	headers[0] = p.Version
	headers[1] = byte(p.Type)
	binary.BigEndian.PutUint32(headers[2:6], p.PayloadLength)

	buf.Write(headers[:])
	for _, block := range p.Blocks {
		if block.Empty() {
			continue
		}

		block.serialize(buf)
	}
}

func (p *Packet) Deserialize(data []byte) error {
	if len(data) < PacketHeaderSize {
		return ErrInvalidPacket
	}

	p.Version = data[0]
	p.Type = PacketType(data[1])
	p.PayloadLength = binary.BigEndian.Uint32(data[2:6])

	return p.DeserializePayload(data[PacketHeaderSize:])
}

func (p *Packet) DeserializePayload(data []byte) error {
	if uint32(len(data)) != p.PayloadLength {
		return ErrInvalidPayloadLength
	}

	blocksData := data

	for len(blocksData) > 0 {
		if len(blocksData) < PayloadBlockHeaderSize {
			return ErrInvalidPacket
		}

		tp := PayloadBlockType(blocksData[0])
		blockLen := binary.BigEndian.Uint16(blocksData[1:3])
		if len(blocksData) < int(PayloadBlockHeaderSize+blockLen) {
			return ErrInvalidPacket
		}

		value := blocksData[3 : 3+blockLen]

		if int(tp) < len(p.Blocks) {
			p.Blocks[tp] = PayloadBlock{
				Type:  tp,
				Len:   blockLen,
				Value: value,
			}
		}

		blocksData = blocksData[3+blockLen:]
	}

	return nil
}

func (p *Packet) FindBlock(tp PayloadBlockType) (PayloadBlock, bool) {
	if int(tp) >= len(p.Blocks) || p.Blocks[tp].Empty() {
		return PayloadBlock{}, false
	}

	return p.Blocks[tp], true
}

func (p *Packet) AddBlock(block PayloadBlock) {
	cur := p.Blocks[block.Type]
	if !cur.Empty() {
		p.PayloadLength -= cur.Size()
	}

	p.Blocks[block.Type] = block
	p.PayloadLength += block.Size()
}

func (p *Packet) Reset() {
	p.Version = 0
	p.Type = 0
	p.PayloadLength = 0
	clear(p.Blocks[:])
}

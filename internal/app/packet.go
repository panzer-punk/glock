package app

import (
	"encoding/binary"
	"errors"
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
	ProtoVersionHeaderSize = 1
	PacketTypeHeaderSize = 1
	PacketPayloadLengthHeaderSize = 4
	PacketHeaderSize = ProtoVersionHeaderSize + PacketTypeHeaderSize + PacketPayloadLengthHeaderSize

	PayloadBlockTypeHeaderSize = 1
	PayloadBlockLengthHeaderSize = 2
	PayloadBlockHeaderSize = PayloadBlockTypeHeaderSize + PayloadBlockLengthHeaderSize
)

type PayloadBlockType uint8

const (
	PayloadBlockTypeNamespace = iota
	PayloadBlockTypeKey
	PayloadBlockTypeValue
	PayloadBlockTypeTTL
	PayloadBlockTypeSecret

	PayloadBlockTypeSuccess //success boolean
	PayloadBlockTypeError //error message
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

type PayloadBlock struct {
	//Headers
	Type  PayloadBlockType
	Len   uint16

	//Payload
	Value []byte
}

func (p *PayloadBlock) Serialize() []byte {
	buf := make([]byte, PayloadBlockHeaderSize+len(p.Value))
	buf[0] = byte(p.Type)
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(p.Value)))
	copy(buf[3:], p.Value)

	return buf
}

func (p *PayloadBlock) Size() uint32 {
	return uint32(PayloadBlockHeaderSize + p.Len)
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
	//@todo maybe use map[PayloadBlockType]PayloadBlock?
	Blocks        []PayloadBlock
}

const (
	ProtoVersion = 1
)

func NewPacket(tp PacketType) Packet {
	return Packet{
		Version:       ProtoVersion,
		Type:          tp,
		PayloadLength: 0,
		Blocks:        make([]PayloadBlock, 0),
	}
}

func (p *Packet) Serialize() []byte {
	payload := make([]byte, 0)
	for _, block := range p.Blocks {
		payload = append(payload, block.Serialize()...)
	}

	p.PayloadLength = uint32(len(payload))

	buf := make([]byte, PacketHeaderSize+len(payload))
	buf[0] = p.Version
	buf[1] = byte(p.Type)
	binary.BigEndian.PutUint32(buf[2:6], p.PayloadLength)
	copy(buf[PacketHeaderSize:], payload)

	return buf
}

func (p *Packet) Deserialize(data []byte) error {
	if len(data) < PacketHeaderSize {
		return errors.New("invalid packet")
	}

	p.Version = data[0]
	p.Type = PacketType(data[1])
	p.PayloadLength = binary.BigEndian.Uint32(data[2:6])

	return p.DeserializePayload(data[PacketHeaderSize:])
}

func (p *Packet) DeserializePayload(data []byte) error {
	p.Blocks = make([]PayloadBlock, 0)

	if uint32(len(data)) != p.PayloadLength {
		return errors.New("invalid packet: payload length mismatch")
	}

	blocksData := data

	for len(blocksData) > 0 {
		if len(blocksData) < PayloadBlockHeaderSize {
			return errors.New("invalid packet")
		}

		tp := blocksData[0]
		blockLen := binary.BigEndian.Uint16(blocksData[1:3])
		if len(blocksData) < int(PayloadBlockHeaderSize+blockLen) {
			return errors.New("invalid packet")
		}

		value := blocksData[3 : 3+blockLen]
		p.Blocks = append(p.Blocks, PayloadBlock{
			Type:  PayloadBlockType(tp),
			Len:   blockLen,
			Value: value,
		})

		blocksData = blocksData[3+blockLen:]
	}

	return nil
}

func (p *Packet) FindBlock(tp PayloadBlockType) (*PayloadBlock, bool) {
	for _, block := range p.Blocks {
		if block.Type == tp {
			return &block, true
		}
	}

	return nil, false
}

func (p *Packet) AddBlock(block PayloadBlock) {
	p.Blocks = append(p.Blocks, block)
	p.PayloadLength += block.Size()
}

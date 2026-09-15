package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func marshal(p Packet) []byte {
	var buf bytes.Buffer
	p.Serialize(&buf)
	return buf.Bytes()
}

func TestNewPacket(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	if p.Version != ProtoVersion {
		t.Fatalf("version: got %d, want %d", p.Version, ProtoVersion)
	}
	if p.Type != PacketTypeLock {
		t.Fatalf("type: got %d, want %d", p.Type, PacketTypeLock)
	}
	if p.PayloadLength != 0 {
		t.Fatalf("payload length: got %d, want 0", p.PayloadLength)
	}
}

func TestAddBlock_UpdatesPayloadLength(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	block := NewPayloadBlock(PayloadBlockTypeKey, []byte("order:1"))
	p.AddBlock(block)

	want := block.Size()
	if p.PayloadLength != want {
		t.Fatalf("payload length: got %d, want %d", p.PayloadLength, want)
	}
}

func TestAddBlock_ReplaceSameType(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	p.AddBlock(NewPayloadBlock(PayloadBlockTypeKey, []byte("a")))
	p.AddBlock(NewPayloadBlock(PayloadBlockTypeKey, []byte("bb")))

	got, ok := p.FindBlock(PayloadBlockTypeKey)
	if !ok {
		t.Fatal("expected key block")
	}
	if string(got.Value) != "bb" {
		t.Fatalf("key: got %q, want bb", got.Value)
	}

	replaced := NewPayloadBlock(PayloadBlockTypeKey, []byte("bb"))
	want := replaced.Size()
	if p.PayloadLength != want {
		t.Fatalf("payload length after replace: got %d, want %d", p.PayloadLength, want)
	}

	raw := marshal(p)
	payloadLen := binary.BigEndian.Uint32(raw[2:6])
	if uint32(len(raw)-PacketHeaderSize) != payloadLen {
		t.Fatalf("wire payload %d, header %d", len(raw)-PacketHeaderSize, payloadLen)
	}
	if payloadLen != p.PayloadLength {
		t.Fatalf("header length %d, packet field %d", payloadLen, p.PayloadLength)
	}
}

func TestFindBlock(t *testing.T) {
	p := NewPacket(PacketTypeSuccess)
	p.AddBlock(NewPayloadBlock(PayloadBlockTypeSuccess, []byte{1}))

	got, ok := p.FindBlock(PayloadBlockTypeSuccess)
	if !ok {
		t.Fatal("expected success block")
	}
	if got.Type != PayloadBlockTypeSuccess || !bytes.Equal(got.Value, []byte{1}) {
		t.Fatalf("unexpected block: %+v", got)
	}

	if _, ok := p.FindBlock(PayloadBlockTypeKey); ok {
		t.Fatal("expected missing key block")
	}
	if _, ok := p.FindBlock(PayloadBlockType(255)); ok {
		t.Fatal("expected out-of-range type to be missing")
	}
}

func TestEmptyBlockNotFoundAndNotSerialized(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	empty := NewPayloadBlock(PayloadBlockTypeNamespace, nil)
	if !empty.Empty() {
		t.Fatal("expected empty block")
	}
	p.AddBlock(empty)

	if _, ok := p.FindBlock(PayloadBlockTypeNamespace); ok {
		t.Fatal("empty block should not be found")
	}

	raw := marshal(p)
	if len(raw) != PacketHeaderSize {
		t.Fatalf("empty block should not be on the wire, got %d bytes", len(raw))
	}
}

func TestSerializeDeserializeRoundtrip(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	p.AddBlock(NewPayloadBlock(PayloadBlockTypeNamespace, []byte("default")))
	p.AddBlock(NewPayloadBlock(PayloadBlockTypeKey, []byte("resource")))
	p.AddBlock(NewPayloadBlock(PayloadBlockTypeTTL, []byte{0, 0, 0, 0, 0, 0, 0, 1}))

	raw := marshal(p)
	if raw[0] != ProtoVersion {
		t.Fatalf("version byte: got %d", raw[0])
	}
	if PacketType(raw[1]) != PacketTypeLock {
		t.Fatalf("type byte: got %d", raw[1])
	}

	var got Packet
	if err := got.Deserialize(raw); err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	if got.Version != p.Version || got.Type != p.Type || got.PayloadLength != p.PayloadLength {
		t.Fatalf("headers: got ver=%d type=%d len=%d", got.Version, got.Type, got.PayloadLength)
	}

	ns, ok := got.FindBlock(PayloadBlockTypeNamespace)
	if !ok || string(ns.Value) != "default" {
		t.Fatalf("namespace: ok=%v value=%q", ok, ns.Value)
	}
	key, ok := got.FindBlock(PayloadBlockTypeKey)
	if !ok || string(key.Value) != "resource" {
		t.Fatalf("key: ok=%v value=%q", ok, key.Value)
	}
	ttl, ok := got.FindBlock(PayloadBlockTypeTTL)
	if !ok || !bytes.Equal(ttl.Value, []byte{0, 0, 0, 0, 0, 0, 0, 1}) {
		t.Fatalf("ttl: ok=%v value=%v", ok, ttl.Value)
	}
}

func TestDeserialize_TooShort(t *testing.T) {
	var p Packet
	err := p.Deserialize([]byte{1, 2, 3})
	if !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("expected ErrInvalidPacket, got %v", err)
	}
}

func TestDeserializePayload_LengthMismatch(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	p.PayloadLength = 4
	err := p.DeserializePayload([]byte{1, 2})
	if !errors.Is(err, ErrInvalidPayloadLength) {
		t.Fatalf("expected ErrInvalidPayloadLength, got %v", err)
	}
}

func TestDeserializePayload_TruncatedBlockHeader(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	payload := []byte{byte(PayloadBlockTypeKey), 0}
	p.PayloadLength = uint32(len(payload))
	err := p.DeserializePayload(payload)
	if !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("expected ErrInvalidPacket, got %v", err)
	}
}

func TestDeserializePayload_TruncatedBlockValue(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	payload := []byte{byte(PayloadBlockTypeKey), 0, 5, 'a', 'b'}
	p.PayloadLength = uint32(len(payload))
	err := p.DeserializePayload(payload)
	if !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("expected ErrInvalidPacket, got %v", err)
	}
}

func TestDeserializePayload_UnknownBlockTypeSkipped(t *testing.T) {
	p := NewPacket(PacketTypeLock)
	key := NewPayloadBlock(PayloadBlockTypeKey, []byte("k"))

	var payload bytes.Buffer
	payload.WriteByte(255)
	payload.Write([]byte{0, 3})
	payload.WriteString("xxx")
	payload.WriteByte(byte(key.Type))
	var ln [2]byte
	binary.BigEndian.PutUint16(ln[:], key.Len)
	payload.Write(ln[:])
	payload.Write(key.Value)

	p.PayloadLength = uint32(payload.Len())
	if err := p.DeserializePayload(payload.Bytes()); err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	got, ok := p.FindBlock(PayloadBlockTypeKey)
	if !ok || string(got.Value) != "k" {
		t.Fatalf("key after unknown block: ok=%v value=%q", ok, got.Value)
	}
}

func TestNewErrPacket(t *testing.T) {
	src := errors.New("boom")
	p := NewErrPacket(src)

	if p.Type != PacketTypeError {
		t.Fatalf("type: got %d, want error", p.Type)
	}

	block, ok := p.FindBlock(PayloadBlockTypeError)
	if !ok {
		t.Fatal("expected error block")
	}
	if string(block.Value) != "boom" {
		t.Fatalf("error message: got %q", block.Value)
	}

	var got Packet
	if err := got.Deserialize(marshal(*p)); err != nil {
		t.Fatalf("roundtrip: %v", err)
	}
	msg, ok := got.FindBlock(PayloadBlockTypeError)
	if !ok || string(msg.Value) != "boom" {
		t.Fatalf("roundtrip message: ok=%v value=%q", ok, msg.Value)
	}
}

func TestWireLayoutMatchesHeaderConstants(t *testing.T) {
	p := NewPacket(PacketTypeTryLock)
	p.AddBlock(NewPayloadBlock(PayloadBlockTypeKey, []byte("ab")))
	raw := marshal(p)

	if len(raw) < PacketHeaderSize {
		t.Fatal("packet shorter than header")
	}
	if raw[0] != ProtoVersion || PacketType(raw[1]) != PacketTypeTryLock {
		t.Fatalf("header prefix: %v", raw[:2])
	}

	payloadLen := binary.BigEndian.Uint32(raw[2:6])
	if int(payloadLen) != len(raw)-PacketHeaderSize {
		t.Fatalf("payload length %d, actual %d", payloadLen, len(raw)-PacketHeaderSize)
	}

	block := raw[PacketHeaderSize:]
	if block[0] != byte(PayloadBlockTypeKey) {
		t.Fatalf("block type: got %d", block[0])
	}
	if binary.BigEndian.Uint16(block[1:3]) != 2 {
		t.Fatalf("block len: got %d", binary.BigEndian.Uint16(block[1:3]))
	}
	if string(block[3:]) != "ab" {
		t.Fatalf("block value: got %q", block[3:])
	}
}

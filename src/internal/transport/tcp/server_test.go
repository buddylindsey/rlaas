package tcp

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"rlaas/src/internal/service"
)

func TestServeConnectionHandlesMultipleMessages(t *testing.T) {
	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()

	server := NewServer("", echoCodec{}, service.NewBasicHandler())
	go server.serveConnection(serverConnection)

	for _, message := range []string{"first", "second"} {
		if err := writeTestFrame(clientConnection, []byte(message)); err != nil {
			t.Fatalf("writeTestFrame() error = %v", err)
		}

		got, err := readTestFrame(clientConnection)
		if err != nil {
			t.Fatalf("readTestFrame() error = %v", err)
		}
		if got, want := string(got), message; got != want {
			t.Errorf("response = %q, want %q", got, want)
		}
	}
}

type echoCodec struct{}

func (echoCodec) Decode(payload []byte) (service.Request, error) {
	return service.Request{
		RequestID: "test-request",
		Type:      service.RequestAcquire,
		Body:      string(payload),
	}, nil
}

func (echoCodec) Encode(response service.Response) ([]byte, error) {
	return []byte(response.Body.(string)), nil
}

func writeTestFrame(connection net.Conn, payload []byte) error {
	var header [frameLengthSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := connection.Write(header[:]); err != nil {
		return err
	}
	_, err := connection.Write(payload)
	return err
}

func readTestFrame(connection net.Conn) ([]byte, error) {
	var header [frameLengthSize]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return nil, err
	}

	payload := make([]byte, binary.BigEndian.Uint32(header[:]))
	if _, err := io.ReadFull(connection, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

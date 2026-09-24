package bufio

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestGSOControlPreservesBatchIO(t *testing.T) {
	for _, connected := range []bool{false, true} {
		for _, disabled := range []bool{false, true} {
			receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer receiver.Close()
			var sender *net.UDPConn
			if connected {
				sender, err = net.DialUDP("udp4", nil, receiver.LocalAddr().(*net.UDPAddr))
			} else {
				sender, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			}
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Close()
			var packetConn N.NetPacketConn
			var socket net.Conn = sender
			if disabled {
				socket = NewUDPConnWithoutGSO(sender)
			}
			if connected {
				// Exercise the wrappers used by protocol batch writers.
				packetConn = NewUnbindPacketConn(NewExtendedConn(socket))
			} else {
				packetConn = NewPacketConn(socket.(net.PacketConn))
			}
			var writer *syscallPacketBatchWriter
			if connected {
				w, ok := CreateConnectedPacketBatchWriter(packetConn)
				if !ok {
					t.Fatal("lost connected batch writer")
				}
				writer, ok = w.(*syscallPacketBatchWriter)
				if !ok {
					t.Fatalf("unexpected batch writer %T", w)
				}
				if _, ok = CreateConnectedPacketBatchReadWaiter(packetConn); !ok {
					t.Fatal("lost connected batch reads")
				}
			} else {
				w, ok := CreatePacketBatchWriter(packetConn)
				if !ok {
					t.Fatal("lost batch writer")
				}
				writer, ok = w.(*syscallPacketBatchWriter)
				if !ok {
					t.Fatalf("unexpected batch writer %T", w)
				}
				if _, ok = CreatePacketBatchReadWaiter(packetConn); !ok {
					t.Fatal("lost batch reads")
				}
			}
			if writer.offload.disabled != disabled {
				t.Fatalf("connected=%v: lost GSO policy", connected)
			}
			payload := bytes.Repeat([]byte{0x42}, 1200)
			buffers := []*buf.Buffer{buf.As(append([]byte(nil), payload...)), buf.As(append([]byte(nil), payload...))}
			if connected {
				err = writer.WriteConnectedPacketBatch(buffers)
			} else {
				destination := M.SocksaddrFromNet(receiver.LocalAddr())
				err = writer.WritePacketBatch(buffers, []M.Socksaddr{destination, destination})
			}
			if err != nil {
				t.Fatal(err)
			}
			if disabled && writer.offload.messages != nil {
				t.Fatal("disabled writer constructed UDP_SEGMENT messages")
			}
			if !disabled && writer.offload.messages == nil {
				t.Fatal("unrestricted writer did not attempt GSO")
			}
			receiver.SetReadDeadline(time.Now().Add(time.Second))
			for range 2 {
				packet := make([]byte, 2000)
				n, _, err := receiver.ReadFrom(packet)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet[:n], payload) {
					t.Fatalf("datagram boundary or content changed: %d bytes", n)
				}
			}
		}
	}
}

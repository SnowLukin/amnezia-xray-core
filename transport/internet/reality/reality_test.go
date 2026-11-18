package reality

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	gotls "crypto/tls"
	"io"
	gonet "net"
	"testing"
	"time"

	goreality "github.com/xtls/reality"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
)

// startDecoy runs the TLS server that REALITY takes its handshake from
func startDecoy(t *testing.T) string {
	t.Helper()
	c, _ := cert.MustGenerate(nil, cert.CommonName("localhost"), cert.DNSNames("localhost"))
	certPEM, keyPEM := c.ToPEM()
	keyPair, err := gotls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := gotls.Listen("tcp", "127.0.0.1:0", &gotls.Config{Certificates: []gotls.Certificate{keyPair}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				io.Copy(io.Discard, c)
				c.Close()
			}()
		}
	}()
	return ln.Addr().String()
}

func TestUClientDropsHandshakeState(t *testing.T) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	shortID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	dest := startDecoy(t)
	serverConfig := (&Config{
		Type:        "tcp",
		Dest:        dest,
		ServerNames: []string{"localhost"},
		PrivateKey:  priv.Bytes(),
		ShortIds:    [][]byte{shortID},
	}).GetREALITYConfig()
	// the server finishes a handshake only once it knows what the decoy sends after one, and it looks again only
	// every 5 seconds. tcp.ListenTCP learns it with DetectPostHandshakeRecordsLens, here it is said to be nothing, under
	// the key that Server looks it up by for each ALPN. A changed key shows as a handshake that never ends
	for _, alpn := range []string{"0", "1", "2"} {
		goreality.GlobalPostHandshakeRecordsLens.Store(dest+" localhost "+alpn, []int{})
	}

	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		s, err := Server(c, serverConfig)
		if err != nil {
			return
		}
		io.Copy(s, s)
	}()

	raw, err := gonet.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(10 * time.Second))
	c, err := UClient(raw, &Config{
		Fingerprint: "chrome",
		ServerName:  "localhost",
		PublicKey:   priv.PublicKey().Bytes(),
		ShortId:     shortID,
	}, context.Background(), net.TCPDestination(net.LocalHostIP, 443))
	if err != nil {
		t.Fatal(err)
	}
	uConn := c.(*UConn)
	if !uConn.Verified {
		t.Fatal("the server was not taken for REALITY")
	}
	// the ML-KEM key of the ClientHello alone is about 9 KiB, and it would stay as long as the connection
	state := uConn.HandshakeState
	if state.Hello != nil || state.ServerHello != nil || state.State13.KeyShareKeys != nil || uConn.Extensions != nil {
		t.Error("the connection keeps what its ClientHello was built from after the handshake")
	}
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "ping" {
		t.Errorf("read %q back instead of ping", got)
	}
}

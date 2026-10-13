package hysteria

import (
	"context"
	"testing"
	"time"

	utls "github.com/refraction-networking/utls"
)

// QUIC handshake cancellation must release the TLS signal channel even when
// the peer has never replied. Successful handshakes don't exercise this path.
func TestChromeParrotTLSCloseBeforePeerReply(t *testing.T) {
	c := utls.UQUICClient(&utls.QUICConfig{TLSConfig: &utls.Config{ServerName: "close.test", InsecureSkipVerify: true}}, utls.HelloCustom)
	if err := c.ApplyPreset(&utls.ClientHelloSpec{
		CipherSuites: []uint16{utls.TLS_AES_128_GCM_SHA256}, CompressionMethods: []byte{0},
		Extensions: []utls.TLSExtension{
			&utls.SupportedVersionsExtension{Versions: []uint16{utls.VersionTLS13}},
			&utls.SupportedCurvesExtension{Curves: []utls.CurveID{utls.X25519}},
			&utls.KeyShareExtension{KeyShares: []utls.KeyShare{{Group: utls.X25519}}},
			&utls.SignatureAlgorithmsExtension{SupportedSignatureAlgorithms: []utls.SignatureScheme{utls.PSSWithSHA256}},
			&utls.ALPNExtension{AlpnProtocols: []string{"h3"}},
			&utls.QUICTransportParametersExtension{},
		},
	}); err != nil {
		t.Fatal(err)
	}
	c.SetTransportParameters([]byte{0x0f, 0})
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { _ = c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("QUIC TLS close retained a pending handshake")
	}
}

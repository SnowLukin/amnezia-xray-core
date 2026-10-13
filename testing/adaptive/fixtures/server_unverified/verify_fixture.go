// Offline guard: a scratch CA grants normal x509 verification only.
package main

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
)

func mustRead(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil { panic(err) }
	return b
}

func main() {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(mustRead(os.Args[1])) || len(pool.Subjects()) != 1 { panic("exactly one CA required") }
	x509.SetFallbackRoots(pool)
	block, _ := pem.Decode(mustRead(os.Args[2]))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil { panic(err) }
	rows := []map[string]any{}
	for _, hostname := range []string{"example.com", "wrong.test"} {
		chains, err := leaf.Verify(x509.VerifyOptions{DNSName: hostname, Intermediates: x509.NewCertPool()})
		message := ""
		if err != nil { message = err.Error() }
		rows = append(rows, map[string]any{"hostname": hostname, "chains": len(chains), "is_verified": err == nil, "error": message})
	}
	json.NewEncoder(os.Stdout).Encode(map[string]any{"godebug": os.Getenv("GODEBUG"), "checks": rows})
}

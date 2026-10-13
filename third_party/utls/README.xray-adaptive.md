Source: github.com/refraction-networking/utls at 6ebdceb2607e (v1.8.3-0.20261006195536-6ebdceb2607e). Runtime Go sources and license only.

Local compatibility changes in u_quic.go: SetTransportParameters updates the extensions cloned by ApplyPreset; Close drains signalc before awaiting the handshake, matching Go 1.27 QUICConn.Close. Keeps Chrome 155 and the rest of the upstream dependencies.

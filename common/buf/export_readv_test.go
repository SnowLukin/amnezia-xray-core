//go:build !wasm && !openbsd

package buf

// ReleaseWhileWaiting lets the tests know whether readv gives its buffers back while it waits here
const ReleaseWhileWaiting = releaseWhileWaiting

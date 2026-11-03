package outbound

// SpeedLimited is a handler whose connections share a byte budget per
// direction. Zero is no limit. Measurements of such a handler show its limit,
// not the capacity of its path.
type SpeedLimited interface {
	SpeedLimit() (uplinkBytesPerSecond, downlinkBytesPerSecond uint64)
}

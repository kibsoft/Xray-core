package routing

// FallbackModeController controls primary/fallback routing mode.
type FallbackModeController interface {
	EnableFallbackMode()
	DisableFallbackMode()
	IsFallbackMode() bool
}

//go:build !linux

package main

// CPU collection is supported on Linux; zero means unavailable on other platforms.
func cpuSeconds() float64 { return 0 }

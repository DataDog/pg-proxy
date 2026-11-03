//go:build !linux

package limiter

func isCgroup2Unified(string) (bool, error) {
	return false, nil
}

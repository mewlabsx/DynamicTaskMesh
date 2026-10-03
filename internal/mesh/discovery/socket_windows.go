//go:build windows

package discovery

import "syscall"

func reuseAddress(_, _ string, connection syscall.RawConn) error {
	var optionErr error
	if err := connection.Control(func(descriptor uintptr) {
		optionErr = syscall.SetsockoptInt(syscall.Handle(descriptor), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	}); err != nil {
		return err
	}
	return optionErr
}

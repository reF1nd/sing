package bufio

import "syscall"

type syscallPacketBatchOffload struct{ disabled bool }

func (o *syscallPacketBatchOffload) reset() {}

func (o *syscallPacketBatchOffload) send(descriptor int, messages []mmsghdr, flags int) (int, syscall.Errno) {
	return sendmmsg(descriptor, messages, flags)
}

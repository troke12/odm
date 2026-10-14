package ffmpeg

import (
	"os/exec"
	"syscall"
)

// createNoWindow keeps the console of a console program from appearing
// when a GUI program starts it.
const createNoWindow = 0x08000000

func hide(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: createNoWindow}
}

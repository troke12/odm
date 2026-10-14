//go:build !windows

package ffmpeg

import "os/exec"

func hide(*exec.Cmd) {}

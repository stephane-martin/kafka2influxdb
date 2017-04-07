package findinit

import "fmt"
import "os"
import "os/exec"
import "strings"
import "bytes"
import "runtime"

type InitType int

// Init systems types
const (
	Systemd InitType = iota
	Upstart
	Bsd
	Sysv
	Openrc
	Launchd
	Unknown
)

// Init systems labels
var InitTypes map[InitType]string = map[InitType]string {
	Systemd: "systemd",
	Upstart: "upstart",
	Bsd: "BSD",
	Sysv: "SysV",
	Openrc: "OpenRC",
	Launchd: "launchd",
	Unknown: "unknown",
}

var InitExePath string = "/sbin/init"

func has_(s1 interface{}, s2 string) bool {
	switch s1 := s1.(type) {
	case string:
		return strings.Contains(strings.ToLower(s1), s2)
	case []byte:
		return bytes.Contains(bytes.ToLower(s1), []byte(s2))
	case fmt.Stringer:
		return strings.Contains(strings.ToLower(s1.String()), s2)
	default:
		return false
	}	
	return false
}

func hasSystemd(s interface{}) bool {
	return has_(s, "systemd")
}

func hasUpstart(s interface{}) bool {
	return has_(s, "upstart")
}

func hasOpenRC(s interface{}) bool {
	return has_(s, "openrc")
}

func which(s interface{}) (InitType, bool) {
	if hasSystemd(s) {
		return Systemd, true
	} else if hasUpstart(s) {
		return Upstart, true
	} else if hasOpenRC(s) {
		return Openrc, true
	}
	return Unknown, false
}


func findInitLinux() InitType {
	out, _ := exec.Command("/sbin/init --version").Output()
	initt, success := which(out)
	if success {
		return initt
	}

	out, _ = exec.Command("/sbin/openrc --version").Output()
	if hasOpenRC(out) {
		return Openrc
	}

	info, err := os.Lstat(InitExePath)
	if err == nil {
		if info.Mode() & os.ModeSymlink != 0 {
			name, err := os.Readlink(InitExePath)
			if err == nil {
				initt, success = which(name)
				if success {
					return initt
				}
			}
		}
	}

	if _, err = os.Stat("/etc/inittab"); err == nil {
		return Sysv
	}

	return Unknown
}


// FindInit returns the type of the current system's init.
func FindInit() InitType {
	switch os := runtime.GOOS; os {
	case "darwin":
		return Launchd
	case "freebsd":
		return Bsd
	case "openbsd":
		return Bsd 
	case "netbsd":
		return Bsd
	case "linux":
		return findInitLinux()
	default:
		return Unknown
	}
}


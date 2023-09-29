package anystore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"unsafe"
)

var (
	DefaultEditors = []string{"/bin/vim", "/usr/bin/vim", "/bin/vi", "/usr/bin/nano"}
)

var (
	ErrNoEditorFound error = errors.New("no editor found")
	ErrNotATerminal  error = errors.New("os.Stdin is not a terminal")
)

// EditThing is an interactive variant of Stash, editing the Thing
// before Stashing it. EditThing does not Unstash before editing, it
// uses the actual Thing. If you need to load Thing from persistence
// before editing, call Unstash prior to EditThing (make sure Thing is
// zero/new/empty before Unstash). EditThing uses encoding/json for
// editing, beware if a user enters "null" on a non-pointer field and
// saves, encoding/json will ignore it effectively using the original
// value without producing an error. Similarily, if a user removes a
// field while editing, the original value will be retained.
//
// Environment variable EDITOR is used as a json editor falling back
// to conf.Editor and finally one of the DefaultEditors.
func EditThing(conf *StashConfig) error {
	if !IsUnixTerminal(os.Stdin) {
		return ErrNotATerminal
	}

	executables := []string{}

	envEditor := os.Getenv("EDITOR")
	switch {
	case envEditor != "" && fileExists(envEditor):
		// Use editor in the EDITOR environment variable
		executables = append(executables, envEditor)
	case conf.Editor == "":
		// Use one of DefaultEditors
		executables = append(executables, DefaultEditors...)
	default:
		// Use conf.Editor
		if !fileExists(conf.Editor) {
			return fmt.Errorf("%q not found", conf.Editor)
		}
		executables = append(executables, conf.Editor)
	}
	if len(executables) == 0 {
		return ErrNoEditorFound
	}

	tempfile, err := dumpThingInTemp(conf.Key, conf.Thing)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	defer func() {
		os.Remove(tempfile)
		signal.Stop(ch)
		close(ch)
	}()

	for {
		err = tryExec(ctx, executables, tempfile)
		if err != nil {
			return err
		}

		if ctx.Err() != nil {
			// Probably unnecessary as tryExec will return error on *exec.ExitError. If
			// terminated by cancel() above, do not read, unmarshal or Stash the
			// tempfile (require clean exit).
			break
		}
		marshalledThing, err := os.ReadFile(tempfile)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(marshalledThing, conf.Thing); err != nil {
			fmt.Printf("Error encoding json: %v\n", err)
			fmt.Printf("Edit file again? [Y/n] ")
		retryQuestion:
			s := bufio.NewScanner(os.Stdin)
			s.Scan()
			if ctx.Err() != nil {
				break
			}
			switch {
			case s.Text() == "", strings.EqualFold(s.Text(), "y"), strings.EqualFold(s.Text(), "yes"):
				continue
			case strings.EqualFold(s.Text(), "n"), strings.EqualFold(s.Text(), "no"):
				return err
			default:
				fmt.Printf("Sorry, please answer yes or no. Edit file again? [Y/n] ")
				goto retryQuestion
			}
		}

		if err := Stash(conf); err != nil {
			return err
		}

		break
	}
	return nil
}

func fileExists(file string) bool {
	fs, err := os.Stat(file)
	if err != nil {
		return false
	}
	if fs.Mode().IsRegular() {
		return true
	}
	return false
}

func dumpThingInTemp(name string, thing any) (string, error) {
	j, err := json.MarshalIndent(thing, "", "  ")
	if err != nil {
		return "", err
	}
	var tempfileTemplate string
	runes := []rune{}
	for _, r := range name {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '-' || r == '_' {
			runes = append(runes, r)
		}
	}
	tempfileTemplate = string(runes) + "-" + rndstr(8) + "-*.json"
	fl, err := os.CreateTemp("", tempfileTemplate)
	if err != nil {
		return "", err
	}
	tempfile := fl.Name()
	remove := true
	defer func() {
		defer fl.Close()
		if remove {
			os.Remove(tempfile)
		}
	}()
	if n, err := fl.Write(j); err != nil {
		return "", err
	} else if n != len(j) {
		return "", ErrWroteTooLittle
	}
	remove = false
	return tempfile, nil
}

func tryExec(ctx context.Context, executables []string, arg ...string) error {
	for _, executable := range executables {
		cmd := exec.CommandContext(ctx, executable, arg...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			if _, found := err.(*exec.ExitError); found {
				return err
			}
			continue
		}
		return nil
	}
	return ErrNoEditorFound
}

// IsUnixTerminal is constructed from terminal.IsTerminal() and is only
// reproduced here in order not to import an external dependency.
func IsUnixTerminal(f *os.File) bool {
	type UnixTermios struct {
		Iflag  uint32
		Oflag  uint32
		Cflag  uint32
		Lflag  uint32
		Line   uint8
		Cc     [19]uint8
		Ispeed uint32
		Ospeed uint32
	}
	const TCGETS = 0x5401
	const SYS_IOCTL = 16
	fd := f.Fd()
	var value UnixTermios
	req := TCGETS
	_, _, e1 := syscall.Syscall(SYS_IOCTL, uintptr(fd), uintptr(req), uintptr(unsafe.Pointer(&value)))
	return e1 == 0
}

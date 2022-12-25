package mr

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

func atomicWriteFile(fileName string, r io.Reader) (err error) {
	// dir and file
	dir, file := filepath.Split(fileName)
	if dir == "" {
		dir = "."
	}
	// create file
	f, err := ioutil.TempFile(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	// remove file if error
	defer func() {
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	// close file
	defer f.Close()
	// copy from r to f
	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cannot write data to temp file %q: %v", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cannot close temp file %q: %v", name, err)
	}

	info, err := os.Stat(fileName)
	if os.IsNotExist(err) {
	} else if err != nil {
		return err
	} else if err := os.Chmod(name, info.Mode()); err != nil {
		return fmt.Errorf("cannot set filemode on temp file %q: %v", name, err)
	}

	if err := os.Rename(name, fileName); err != nil {
		return fmt.Errorf("cannot replace %q with temp file %q: %v", fileName, name, err)
	}
	return nil
}

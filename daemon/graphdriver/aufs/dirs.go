// +build linux

package aufs

import (
	"bufio"
	"io/ioutil"
	"os"
	"path"
)

// Return all files inside of given root
func loadIds(root string) ([]string, error) {
	return getChilds(root, 1)
}

func getChildDirs(root string) ([]string, error) {
	return getChilds(root, 2)
}

// includeType:
//	0 - files and directories
//	1 - only files
//	2 - only directories
func getChilds(root string, includeType int) ([]string, error) {
	dirs, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, err
	}
	out := []string{}
	for _, d := range dirs {
		if includeType == 1 && d.IsDir() {
			continue
		}

		if includeType == 2 && !d.IsDir() {
			continue
		}

		out = append(out, d.Name())
	}

	return out, nil
}

// Read the layers file for the current id and return all the
// layers represented by new lines in the file
//
// If there are no lines in the file then the id has no parent
// and an empty slice is returned.
func getParentIds(root, id string) ([]string, error) {
	f, err := os.Open(path.Join(root, "layers", id))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := []string{}
	s := bufio.NewScanner(f)

	for s.Scan() {
		if t := s.Text(); t != "" {
			out = append(out, s.Text())
		}
	}
	return out, s.Err()
}

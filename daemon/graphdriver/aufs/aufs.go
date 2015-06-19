// +build linux

/*

aufs driver directory structure

  .
  ├── layers // Metadata of layers
  │   ├── 1
  │   ├── 2
  │   └── 3
  ├── diff  // Content of the layer
  │   ├── 1  // Contains layers that need to be mounted for the id
  │   ├── 2
  │   └── 3
  └── mnt    // Mount points for the rw layers to be mounted
      ├── 1
      ├── 2
      └── 3

*/

package aufs

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/chrootarchive"
	"github.com/docker/docker/pkg/directory"
	mountpk "github.com/docker/docker/pkg/mount"
	"github.com/docker/docker/pkg/stringid"
	"github.com/docker/libcontainer/label"
)

var (
	ErrAufsNotSupported = fmt.Errorf("AUFS was not found in /proc/filesystems")
	incompatibleFsMagic = []graphdriver.FsMagic{
		graphdriver.FsMagicBtrfs,
		graphdriver.FsMagicAufs,
	}
	backingFs = "<unknown>"

	enableDirpermLock sync.Once
	enableDirperm     bool

	rootNFSImageLayers       = "/mnt"
	rootLocalContainerLayers = "/var/lib/docker-aufs/container-layers"
	rootLocalImageLayers     = "/var/lib/docker-aufs/image-layers"
)

func init() {
	graphdriver.Register("aufs", Init)
}

type Driver struct {
	rootNFSImageLayers       string // NFS root of remote ImageLayers
	rootLocalContainerLayers string // R/W directory for local conatiner layers
	rootLocalImageLayers     string // R/W directory for local ImageLayers
	sync.Mutex                      // Protects concurrent modification to active
	active                   map[string]int
}

type IdDesc struct {
	id       string
	rootPath string
}

// New returns a new AUFS driver.
// An error is returned if AUFS is not supported.
func Init(root string, options []string) (graphdriver.Driver, error) {

	// Try to load the aufs kernel module
	if err := supportsAufs(); err != nil {
		return nil, graphdriver.ErrNotSupported
	}

	fsMagic, err := graphdriver.GetFSMagic(root)
	if err != nil {
		return nil, err
	}
	if fsName, ok := graphdriver.FsNames[fsMagic]; ok {
		backingFs = fsName
	}

	for _, magic := range incompatibleFsMagic {
		if fsMagic == magic {
			return nil, graphdriver.ErrIncompatibleFS
		}
	}

	a := &Driver{
		rootNFSImageLayers:       rootNFSImageLayers,
		rootLocalContainerLayers: rootLocalContainerLayers,
		rootLocalImageLayers:     rootLocalImageLayers,
		active:                   make(map[string]int),
	}

	if err := createRootDir(a.rootLocalContainerLayers); err != nil {
		return nil, err
	}

	if err := createRootDir(a.rootLocalImageLayers); err != nil {
		return nil, err
	}

	// TODO: Here we also need to check that a.rootNFSImageLayers exists

	return a, nil
}

func createRootDir(root string) error {
	paths := []string{
		"mnt",
		"diff",
		"layers",
	}

	if err := os.MkdirAll(root, 0755); err != nil {
		if os.IsExist(err) {
			return nil
		}

		return err
	}

	for _, p := range paths {
		if err := os.MkdirAll(path.Join(root, p), 0755); err != nil {
			return err
		}
	}

	if err := mountpk.MakePrivate(root); err != nil {
		return err
	}

	return nil
}

// Return a nil error if the kernel supports aufs
// We cannot modprobe because inside dind modprobe fails
// to run
func supportsAufs() error {
	// We can try to modprobe aufs first before looking at
	// proc/filesystems for when aufs is supported
	exec.Command("modprobe", "aufs").Run()

	f, err := os.Open("/proc/filesystems")
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if strings.Contains(s.Text(), "aufs") {
			return nil
		}
	}
	return ErrAufsNotSupported
}

func lookupId(targetId string, ids []string) *IdDesc {
	for _, id := range ids {
		if id == targetId {
			idDesc := &IdDesc{
				id: id,
			}

			return idDesc
		}
	}

	return nil
}

func (a *Driver) getIdDesc(targetId string) (*IdDesc, error) {
	// first, lookup targetId locally
	paths := []string{
		a.rootLocalContainerLayers,
		a.rootLocalImageLayers,
	}

	for _, p := range paths {
		ids, err := loadIds(path.Join(p, "layers"))
		if err != nil {
			return nil, err
		}

		idDesc := lookupId(targetId, ids)
		if idDesc != nil {
			idDesc.rootPath = p
			return idDesc, nil
		}
	}

	// targetId does not exist locally, lookup remote dest
	dirs, err := getChildDirs(a.rootNFSImageLayers)
	if err != nil {
		return nil, err
	}

	for _, subdir := range dirs {
		ids, err := loadIds(path.Join(a.rootNFSImageLayers, subdir, "layers"))
		if err != nil {
			continue
		}

		idDesc := lookupId(targetId, ids)
		if idDesc != nil {
			idDesc.rootPath = path.Join(a.rootNFSImageLayers, subdir)
			return idDesc, nil
		}
	}

	return nil, fmt.Errorf("Unknown ID")
}

func (a *Driver) rootPath() string {
	// FIXME: Temporary hack to support local migration (see migrate.go)
	return a.rootLocalImageLayers
}

func (*Driver) String() string {
	return "aufs"
}

func (a *Driver) Status() [][2]string {
	ids, _ := a.loadAllIds()
	return [][2]string{
		{"Local Image Layers Root Dir", a.rootLocalImageLayers},
		{"Local Container Layers Root Dir", a.rootLocalContainerLayers},
		{"NFS Image Layers Root Dir", a.rootNFSImageLayers},
		{"Backing Filesystem", backingFs},
		{"Dirs", fmt.Sprintf("%d", len(ids))},
		{"Dirperm1 Supported", fmt.Sprintf("%v", useDirperm())},
	}
}

func (a *Driver) GetMetadata(id string) (map[string]string, error) {
	return nil, nil
}

// Exists returns true if the given id is registered with
// this driver
func (a *Driver) Exists(id string) bool {
	if _, err := a.getIdDesc(id); err == nil {
		return true
	}

	return false
}

// Three folders are created for each id
// mnt, layers, and diff
func (a *Driver) Create(id, parent string, isImageLayer bool) error {
	rootPath, err := a.createDirsFor(id, isImageLayer)
	if err != nil {
		return err
	}

	// Write the layers metadata
	f, err := os.Create(path.Join(rootPath, "layers", id))
	if err != nil {
		return err
	}
	defer f.Close()

	if parent != "" {
		parentIdDesc, err := a.getIdDesc(parent)
		if err != nil {
			return err
		}

		ids, err := getParentIds(parentIdDesc.rootPath, parentIdDesc.id)
		if err != nil {
			return err
		}

		if _, err := fmt.Fprintln(f, parent); err != nil {
			return err
		}
		for _, i := range ids {
			if _, err := fmt.Fprintln(f, i); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Driver) createDirsFor(id string, isImageLayer bool) (string, error) {
	paths := []string{
		"mnt",
		"diff",
	}

	rootPath := a.rootLocalLayers(isImageLayer)

	for _, p := range paths {
		if err := os.MkdirAll(path.Join(rootPath, p, id), 0755); err != nil {
			return "", err
		}
	}
	return rootPath, nil
}

func (a *Driver) rootLocalLayers(isImageLayer bool) string {
	var rootPath string
	if isImageLayer {
		rootPath = a.rootLocalImageLayers
	} else {
		rootPath = a.rootLocalContainerLayers
	}

	return rootPath
}

// Unmount and remove the dir information
func (a *Driver) Remove(id string) error {
	// Protect the a.active from concurrent access
	a.Lock()
	defer a.Unlock()

	idDesc, err := a.getIdDesc(id)
	if err != nil {
		return err
	}

	if a.active[idDesc.id] != 0 {
		logrus.Errorf("Removing active id %s", idDesc.id)
	}

	// Make sure the dir is umounted first
	if err := a.unmount(idDesc); err != nil {
		return err
	}
	tmpDirs := []string{
		"mnt",
		"diff",
	}

	// Atomically remove each directory in turn by first moving it out of the
	// way (so that docker doesn't find it anymore) before doing removal of
	// the whole tree.
	for _, p := range tmpDirs {

		realPath := path.Join(idDesc.rootPath, p, idDesc.id)
		tmpPath := path.Join(idDesc.rootPath, p, fmt.Sprintf("%s-removing", idDesc.id))
		if err := os.Rename(realPath, tmpPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		defer os.RemoveAll(tmpPath)
	}

	// Remove the layers file for the id
	if err := os.Remove(path.Join(idDesc.rootPath, "layers", idDesc.id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Return the rootfs path for the id
// This will mount the dir at it's given path
func (a *Driver) Get(id, mountLabel string) (string, error) {
	idDesc, err := a.getIdDesc(id)
	if err != nil {
		return "", err
	}

	ids, err := getParentIds(idDesc.rootPath, idDesc.id)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", err
		}
		ids = []string{}
	}

	// Protect the a.active from concurrent access
	a.Lock()
	defer a.Unlock()

	count := a.active[id]

	// If a dir does not have a parent ( no layers )do not try to mount
	// just return the diff path to the data
	out := path.Join(idDesc.rootPath, "diff", idDesc.id)
	if len(ids) > 0 {
		out = path.Join(idDesc.rootPath, "mnt", idDesc.id)

		if count == 0 {
			if err := a.mount(idDesc, mountLabel); err != nil {
				return "", err
			}
		}
	}

	a.active[id] = count + 1

	return out, nil
}

func (a *Driver) Put(id string) error {
	idDesc, err := a.getIdDesc(id)
	if err != nil {
		return err
	}

	// Protect the a.active from concurrent access
	a.Lock()
	defer a.Unlock()

	if count := a.active[idDesc.id]; count > 1 {
		a.active[idDesc.id] = count - 1
	} else {
		ids, _ := getParentIds(idDesc.rootPath, idDesc.id)
		// We only mounted if there are any parents
		if ids != nil && len(ids) > 0 {
			a.unmount(idDesc)
		}
		delete(a.active, idDesc.id)
	}
	return nil
}

// Diff produces an archive of the changes between the specified
// layer and its parent layer which may be "".
func (a *Driver) Diff(id, parent string) (archive.Archive, error) {
	idDesc, err := a.getIdDesc(id)
	if err != nil {
		return nil, err
	}

	// AUFS doesn't need the parent layer to produce a diff.
	return archive.TarWithOptions(path.Join(idDesc.rootPath, "diff", idDesc.id),
		&archive.TarOptions{
			Compression:     archive.Uncompressed,
			ExcludePatterns: []string{".wh..wh.*"},
		})
}

func (a *Driver) applyDiff(id string, diff archive.ArchiveReader) error {
	idDesc, err := a.getIdDesc(id)
	if err != nil {
		return err
	}
	return chrootarchive.Untar(diff, path.Join(idDesc.rootPath, "diff", idDesc.id), nil)
}

// DiffSize calculates the changes between the specified id
// and its parent and returns the size in bytes of the changes
// relative to its base filesystem directory.
func (a *Driver) DiffSize(id, parent string) (size int64, err error) {
	idDesc, err := a.getIdDesc(id)
	if err != nil {
		return 0, err
	}

	// AUFS doesn't need the parent layer to calculate the diff size.
	return directory.Size(path.Join(idDesc.rootPath, "diff", idDesc.id))
}

// ApplyDiff extracts the changeset from the given diff into the
// layer with the specified id and parent, returning the size of the
// new layer in bytes.
func (a *Driver) ApplyDiff(id, parent string, diff archive.ArchiveReader) (size int64, err error) {
	// AUFS doesn't need the parent id to apply the diff.
	if err = a.applyDiff(id, diff); err != nil {
		return
	}

	return a.DiffSize(id, parent)
}

// Changes produces a list of changes between the specified layer
// and its parent layer. If parent is "", then all changes will be ADD changes.
func (a *Driver) Changes(id, parent string) ([]archive.Change, error) {
	idDesc, err := a.getIdDesc(id)
	if err != nil {
		return nil, err
	}
	// AUFS doesn't have snapshots, so we need to get changes from all parent
	// layers.
	layers, err := a.getParentLayerPaths(idDesc)
	if err != nil {
		return nil, err
	}
	return archive.Changes(layers, path.Join(idDesc.rootPath, "diff", idDesc.id))
}

func (a *Driver) getParentLayerPaths(idDesc *IdDesc) ([]string, error) {
	parentIds, err := getParentIds(idDesc.rootPath, idDesc.id)
	if err != nil {
		return nil, err
	}

	layers := make([]string, len(parentIds))

	// Get the diff paths for all the parent ids
	for i, p := range parentIds {
		parentIdDesc, err := a.getIdDesc(p)
		if err != nil {
			return nil, err
		}

		layers[i] = path.Join(parentIdDesc.rootPath, "diff", parentIdDesc.id)
	}

	return layers, nil
}

func (a *Driver) mount(idDesc *IdDesc, mountLabel string) error {
	// If the id is mounted or we get an error return
	if mounted, err := a.mounted(idDesc); err != nil || mounted {
		return err
	}

	var (
		target = path.Join(idDesc.rootPath, "mnt", idDesc.id)
		rw     = path.Join(idDesc.rootPath, "diff", idDesc.id)
	)

	layers, err := a.getParentLayerPaths(idDesc)
	if err != nil {
		return err
	}

	if err := a.aufsMount(layers, rw, target, mountLabel); err != nil {
		return fmt.Errorf("error creating aufs mount to %s: %v", target, err)
	}
	return nil
}

func (a *Driver) unmount(idDesc *IdDesc) error {
	if mounted, err := a.mounted(idDesc); err != nil || !mounted {
		return err
	}

	target := path.Join(idDesc.rootPath, "mnt", idDesc.id)
	return Unmount(target)
}

func (a *Driver) mounted(idDesc *IdDesc) (bool, error) {
	target := path.Join(idDesc.rootPath, "mnt", idDesc.id)
	return mountpk.Mounted(target)
}

// During cleanup aufs needs to unmount all mountpoints
func (a *Driver) Cleanup() error {
	ids, err := a.loadAllIds()
	if err != nil {
		return err
	}

	for _, id := range ids {
		idDesc, err := a.getIdDesc(id)
		if err != nil {
			logrus.Errorf("Unknown ID: %s", id)
			continue
		}

		if err := a.unmount(idDesc); err != nil {
			logrus.Errorf("Unmounting %s: %s", stringid.TruncateID(id), err)
		}
	}

	mountpk.Unmount(a.rootLocalContainerLayers)
	mountpk.Unmount(a.rootLocalImageLayers)

	return nil
}

func (a *Driver) loadAllIds() ([]string, error) {
	var allIds []string

	// first, local
	paths := []string{
		a.rootLocalContainerLayers,
		a.rootLocalImageLayers,
	}

	for _, p := range paths {
		ids, err := loadIds(path.Join(p, "layers"))
		if err != nil {
			return nil, err
		}

		allIds = append(allIds, ids...)
	}

	// add remote
	dirs, err := getChildDirs(a.rootNFSImageLayers)
	if err != nil {
		return allIds, nil
	}

	for _, subdir := range dirs {
		ids, err := loadIds(path.Join(a.rootNFSImageLayers, subdir, "layers"))
		if err != nil {
			continue
		}

		allIds = append(allIds, ids...)
	}

	return allIds, nil
}

func (a *Driver) aufsMount(ro []string, rw, target, mountLabel string) (err error) {
	defer func() {
		if err != nil {
			Unmount(target)
		}
	}()

	// Mount options are clipped to page size(4096 bytes). If there are more
	// layers then these are remounted individually using append.

	offset := 54
	if useDirperm() {
		offset += len("dirperm1")
	}
	b := make([]byte, syscall.Getpagesize()-len(mountLabel)-offset) // room for xino & mountLabel
	bp := copy(b, fmt.Sprintf("br:%s=rw", rw))

	firstMount := true
	i := 0

	for {
		for ; i < len(ro); i++ {
			layer := fmt.Sprintf(":%s=ro+wh", ro[i])

			if firstMount {
				if bp+len(layer) > len(b) {
					break
				}
				bp += copy(b[bp:], layer)
			} else {
				data := label.FormatMountLabel(fmt.Sprintf("append%s", layer), mountLabel)
				if err = mount("none", target, "aufs", MsRemount, data); err != nil {
					return
				}
			}
		}

		if firstMount {
			opts := "dio,xino=/dev/shm/aufs.xino"
			if useDirperm() {
				opts += ",dirperm1"
			}
			data := label.FormatMountLabel(fmt.Sprintf("%s,%s", string(b[:bp]), opts), mountLabel)
			if err = mount("none", target, "aufs", 0, data); err != nil {
				return
			}
			firstMount = false
		}

		if i == len(ro) {
			break
		}
	}

	return
}

// useDirperm checks dirperm1 mount option can be used with the current
// version of aufs.
func useDirperm() bool {
	enableDirpermLock.Do(func() {
		base, err := ioutil.TempDir("", "docker-aufs-base")
		if err != nil {
			logrus.Errorf("error checking dirperm1: %v", err)
			return
		}
		defer os.RemoveAll(base)

		union, err := ioutil.TempDir("", "docker-aufs-union")
		if err != nil {
			logrus.Errorf("error checking dirperm1: %v", err)
			return
		}
		defer os.RemoveAll(union)

		opts := fmt.Sprintf("br:%s,dirperm1,xino=/dev/shm/aufs.xino", base)
		if err := mount("none", union, "aufs", 0, opts); err != nil {
			return
		}
		enableDirperm = true
		if err := Unmount(union); err != nil {
			logrus.Errorf("error checking dirperm1: failed to unmount %v", err)
		}
	})
	return enableDirperm
}

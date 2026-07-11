//go:build fat_noexfat || fat_nolfn

package fat

// exfatEnabled reports whether this build has exFAT support (FatFs'
// FF_FS_EXFAT). See exfat.go for the enabled implementation.
const exfatEnabled = false

// dirbuffer is zero-sized without exFAT support, reclaiming 608 bytes per FS.
type dirbuffer = [0]byte

func (fsys *FS) init_exfat() fileResult { return frUnsupported }

func (fs *FS) change_bitmap(clst, ncl uint32, bv bool) fileResult { return frUnsupported }

func (obj *objid) init_alloc_info() {}

func (obj *objid) clusterstat_exfat(clst uint32) uint32 { return 1 }

func (dp *dir) read_exfat(vol bool) fileResult { return frUnsupported }

func (dp *dir) find_exfat() fileResult { return frUnsupported }

func (dp *dir) get_fileinfo_exfat(fno *FileInfo) {}

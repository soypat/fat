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

func (obj *objid) create_chain_exfat(clst, scl uint32) (uint32, fileResult) { return 1, frOK }

func (obj *objid) remove_chain_exfat_post(pclst uint32) fileResult { return frUnsupported }

func (dp *dir) read_exfat(vol bool) fileResult { return frUnsupported }

func (dp *dir) find_exfat() fileResult { return frUnsupported }

func (dp *dir) register_exfat() fileResult { return frUnsupported }

func (obj *objid) fill_last_frag(lcl, term uint32) fileResult { return frUnsupported }

func (fsys *FS) f_sync_exfat(fp *File, tm uint32) fileResult { return frUnsupported }

func (fp *File) open_trunc_exfat(dj *dir, tm uint32) fileResult { return frUnsupported }

func (dj *dir) mkdir_fin_exfat(dcl, tm uint32) fileResult { return frUnsupported }

func (djn *dir) rename_restore_exfat(buf *[2 * sizeDirEntry]byte) fileResult { return frUnsupported }

func (f *Formatter) formatExFAT(blocksize, fsSizeInBlocks int, cfg FormatConfig) error {
	return frUnsupported
}

func (dp *dir) get_fileinfo_exfat(fno *FileInfo) {}

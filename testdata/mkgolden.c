// mkgolden.c — deterministic FatFs torture-image generator.
//
// Produces six images in the given output directory:
//
//   golden-fmt12.img      FAT12 4MiB volume (au 4096) right after f_mkfs (baseline)
//   golden-torture12.img  same volume after the small torture script
//   golden-fmt16.img      FAT16 4MiB volume (au 512) right after f_mkfs (baseline)
//   golden-torture16.img  same volume after the small torture script
//   golden-fmt32.img      FAT32 64MiB volume (au 512) right after f_mkfs (baseline)
//   golden-torture32.img  same volume after the FAT32 torture script
//
// golden_torture_test.go loads a baseline image (the Go port has no mkfs),
// replays the exact same numbered script through the exported Go API and
// requires the resulting image to match the corresponding torture image byte
// for byte. The numbered steps here and in the Go test must stay in lockstep.
//
// The block device matches the Go test BlockByteSlice: writes are persistent
// and reads return exactly what was written.
//
// Required ffconf.h settings (already applied to local/ff16/source/ffconf.h):
//   FF_USE_MKFS 1, FF_USE_LFN 1, FF_LFN_UNICODE 2 (UTF-8), FF_CODE_PAGE 437
//
// get_fattime() returns 0 to match the Go port's FS.time().
//
// Build & run from the fat/ directory:
//   gcc -O1 -o /tmp/mkgolden testdata/mkgolden.c local/ff16/source/ff.c \
//       local/ff16/source/ffunicode.c -Ilocal/ff16/source
//   /tmp/mkgolden testdata
//   gzip -9nf testdata/golden-*.img

#include "ff.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Provide the DSTATUS/DRESULT/RES_* definitions that diskio.h would have
// provided. This avoids pulling in diskio.c (and its platform.h dependency).
typedef BYTE DSTATUS;
typedef enum {
	RES_OK = 0, RES_ERROR, RES_WRPRT, RES_NOTRDY, RES_PARERR
} DRESULT;

#define GET_SECTOR_COUNT	1
#define GET_SECTOR_SIZE		2
#define GET_BLOCK_SIZE		3
#define CTRL_SYNC		0

#define SECTOR_SIZE 512
#define NUM_SECTORS16 8192    /* 4 MiB image, au 4096 -> FAT16 */
#define NUM_SECTORS32 131072  /* 64 MiB image, au 512 -> FAT32 */
#define WORK_SIZE   8192

static uint8_t* g_mem;
static DWORD g_sectors;
static FATFS fs;
static FIL fil;

// Block device callbacks for FatFs.
DSTATUS disk_initialize(BYTE pdrv) { (void)pdrv; return 0; }
DSTATUS disk_status(BYTE pdrv)     { (void)pdrv; return 0; }
DRESULT disk_read(BYTE pdrv, BYTE* buf, LBA_t sector, UINT count) {
    (void)pdrv;
    memcpy(buf, &g_mem[sector * SECTOR_SIZE], count * SECTOR_SIZE);
    return RES_OK;
}
DRESULT disk_write(BYTE pdrv, const BYTE* buf, LBA_t sector, UINT count) {
    (void)pdrv;
    memcpy(&g_mem[sector * SECTOR_SIZE], buf, count * SECTOR_SIZE);
    return RES_OK;
}
DRESULT disk_ioctl(BYTE pdrv, BYTE cmd, void* buff) {
    (void)pdrv;
    if (cmd == GET_SECTOR_COUNT) { *(DWORD*)buff = g_sectors;   return RES_OK; }
    if (cmd == GET_SECTOR_SIZE)  { *(WORD*)buff  = SECTOR_SIZE; return RES_OK; }
    if (cmd == GET_BLOCK_SIZE)   { *(DWORD*)buff = 1;           return RES_OK; }
    if (cmd == CTRL_SYNC)        { return RES_OK; }
    return RES_PARERR;
}

DWORD get_fattime(void) { return 0; } /* Matches Go port FS.time(). */

// Deterministic data pattern generator (must match the Go side).
static uint8_t pat(int tag, int i) {
    return (uint8_t)(i * 31 + tag * 17 + 7);
}

static void check(FRESULT fr, const char* what) {
    if (fr != FR_OK) {
        fprintf(stderr, "mkgolden: %s: %d\n", what, fr);
        exit(1);
    }
}

// write_pat writes n patterned bytes to the open file; the pattern index is
// the absolute file offset start+.. so content is chunk-size independent.
static void write_pat(FIL* f, int tag, int start, int n) {
    uint8_t buf[512];
    for (int done = 0; done < n; ) {
        int c = (n - done < (int)sizeof(buf)) ? n - done : (int)sizeof(buf);
        for (int j = 0; j < c; j++) {
            buf[j] = pat(tag, start + done + j);
        }
        UINT bw;
        if (f_write(f, buf, c, &bw) != FR_OK || bw != (UINT)c) {
            fprintf(stderr, "write_pat failed\n");
            exit(1);
        }
        done += c;
    }
}

// create_pat creates/truncates a file and writes n patterned bytes.
static void create_pat(const char* name, int tag, int n) {
    check(f_open(&fil, name, FA_CREATE_ALWAYS | FA_WRITE), name);
    write_pat(&fil, tag, 0, n);
    check(f_close(&fil), name);
}

// append_pat opens an existing file in append mode and writes n bytes
// continuing the pattern at offset start (must equal current file size).
static void append_pat(const char* name, int tag, int start, int n) {
    check(f_open(&fil, name, FA_OPEN_APPEND | FA_WRITE), name);
    if (f_size(&fil) != (FSIZE_t)start) {
        fprintf(stderr, "append_pat %s: size %lu != start %d\n", name, (unsigned long)f_size(&fil), start);
        exit(1);
    }
    write_pat(&fil, tag, start, n);
    check(f_close(&fil), name);
}

static void write_str(const char* name, const char* content) {
    UINT bw;
    check(f_open(&fil, name, FA_CREATE_ALWAYS | FA_WRITE), name);
    check(f_write(&fil, content, (UINT)strlen(content), &bw), name);
    check(f_close(&fil), name);
}

static void dump(const char* dir, const char* name, size_t size) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE* f = fopen(path, "wb");
    if (!f) { perror(path); exit(1); }
    if (fwrite(g_mem, 1, size, f) != size) {
        fprintf(stderr, "short write of %s\n", path);
        exit(1);
    }
    fclose(f);
    printf("wrote %s (%zu bytes)\n", path, size);
}

/* ---------------- small (FAT12/FAT16) torture script ----------------
   Steps must stay in lockstep with TestGoldenTortureFAT12/FAT16. Sizes are
   in bytes; comments assume the FAT12 image's 4096-byte clusters. */
static void script_small(void) {
    char name[64];
    int i;

    /* S1: a.dat = 3 clusters (tag 1). Cluster size is 4096. */
    create_pat("a.dat", 1, 3 * 4096);

    /* S2: b.dat = 2 clusters (tag 2). */
    create_pat("b.dat", 2, 2 * 4096);

    /* S3: extend a.dat by 2 clusters in append mode. */
    append_pat("a.dat", 1, 3 * 4096, 2 * 4096);

    /* S4: frag.dat = 1 cluster, then append 1 more (fragmentation). */
    create_pat("frag.dat", 3, 4096);
    append_pat("frag.dat", 3, 4096, 4096);

    /* S5: interleaved sub-cluster appends to a.dat/b.dat (4 rounds of
       1024B each) so their cluster chains interleave as they grow. */
    for (i = 0; i < 4; i++) {
        append_pat("a.dat", 1, 5 * 4096 + i * 1024, 1024);
        append_pat("b.dat", 2, 2 * 4096 + i * 1024, 1024);
    }

    /* S6: punch FAT holes. */
    check(f_unlink("b.dat"), "unlink b.dat");
    check(f_unlink("frag.dat"), "unlink frag.dat");

    /* S7: c.dat = 6 clusters threads through the holes (tag 4). */
    create_pat("c.dat", 4, 6 * 4096);

    /* S8: re-create a.dat with CREATE_ALWAYS (truncates old chain,
       allocator reuses the hole) and write 2 clusters (tag 5). */
    create_pat("a.dat", 5, 2 * 4096);

    /* S9: mid-file overwrite of c.dat at misaligned offset 12345 (tag 6),
       then seek past EOF to 40000 and write 3000 (tag 7): chain stretch. */
    check(f_open(&fil, "c.dat", FA_READ | FA_WRITE), "open c.dat");
    check(f_lseek(&fil, 12345), "seek c.dat 12345");
    write_pat(&fil, 6, 0, 5000);
    check(f_lseek(&fil, 40000), "seek c.dat 40000");
    write_pat(&fil, 7, 0, 3000);
    check(f_sync(&fil), "sync c.dat");
    check(f_close(&fil), "close c.dat");

    /* S10: 12 LFNs colliding on the same 8.3 stem: numbered ~1..~5 then
       hashed short names. */
    for (i = 1; i <= 12; i++) {
        snprintf(name, sizeof(name), "collision test file %02d.dat", i);
        write_str(name, "collide");
    }

    /* S11: unicode LFNs (UTF-8 paths; CP437 SFN conversion incl. lossy). */
    check(f_open(&fil, "\xc3\xb1""and\xc3\xba.txt", FA_CREATE_ALWAYS | FA_WRITE), "nandu"); /* ñandú.txt */
    write_pat(&fil, 8, 0, 100);
    check(f_close(&fil), "nandu close");
    check(f_open(&fil, "\xe6\x9b\xb2\xe3\x81\x8c\xe3\x82\x8a\xe8\xa7\x92.txt", FA_CREATE_ALWAYS | FA_WRITE), "magarikado"); /* 曲がり角.txt */
    write_pat(&fil, 8, 0, 200);
    check(f_close(&fil), "magarikado close");
    check(f_open(&fil, "\xce\xb1\xce\xb2\xce\xb3\xce\xb4\xce\xb5.dat", FA_CREATE_ALWAYS | FA_WRITE), "greek"); /* αβγδε.dat */
    write_pat(&fil, 8, 0, 300);
    check(f_close(&fil), "greek close");
    check(f_open(&fil, "\xf0\x9f\x98\x80""emoji\xf0\x9f\x98\x80.txt", FA_CREATE_ALWAYS | FA_WRITE), "emoji"); /* 😀emoji😀.txt (surrogate pairs) */
    write_pat(&fil, 8, 0, 50);
    check(f_close(&fil), "emoji close");

    /* S12: 32 small files. */
    for (i = 0; i < 32; i++) {
        snprintf(name, sizeof(name), "small%02d.txt", i);
        write_str(name, "hello");
    }

    /* S13: delete every other small file. */
    for (i = 0; i < 32; i += 2) {
        snprintf(name, sizeof(name), "small%02d.txt", i);
        check(f_unlink(name), name);
    }

    /* S14: 8 new files reuse the freed directory slots. */
    for (i = 0; i < 8; i++) {
        snprintf(name, sizeof(name), "renew%02d.txt", i);
        write_str(name, "world!");
    }

    /* S15: delete an LFN entry block in the middle of the directory. */
    check(f_unlink("collision test file 07.dat"), "unlink collision 07");
}

/* ---------------- FAT32 torture script ----------------
   Steps must stay in lockstep with TestGoldenTortureFAT32.
   Cluster size is 512B so chains are long and the root directory
   (a cluster chain on FAT32) stretches often. */
static void script32(void) {
    char name[64];
    int i;

    /* S1: 140 LFN files in the root directory force repeated root-dir
       stretch (each file needs 4 dir entries = 1/4 cluster). */
    for (i = 0; i < 140; i++) {
        snprintf(name, sizeof(name), "root%03d file with a long name.txt", i);
        char content[32];
        snprintf(content, sizeof(content), "file %03d", i);
        write_str(name, content);
    }

    /* S2: big.dat = 100000 bytes (tag 1), ~196 cluster chain. */
    create_pat("big.dat", 1, 100000);

    /* S3: remove every third root file: punches LFN-block holes in the
       stretched root directory and frees clusters. */
    for (i = 0; i < 140; i += 3) {
        snprintf(name, sizeof(name), "root%03d file with a long name.txt", i);
        check(f_unlink(name), name);
    }

    /* S4: append 50000 bytes to big.dat. */
    append_pat("big.dat", 1, 100000, 50000);

    /* S5: huge.dat = 150000 bytes (tag 2). */
    create_pat("huge.dat", 2, 150000);

    /* S6: mid-file overwrite of big.dat at offset 100000 minus a bit,
       misaligned (tag 3). */
    check(f_open(&fil, "big.dat", FA_READ | FA_WRITE), "open big.dat");
    check(f_lseek(&fil, 99991), "seek big.dat");
    write_pat(&fil, 3, 0, 8000);
    check(f_close(&fil), "close big.dat");

    /* S7: 30 new LFN files fill root-dir holes and stretch further. */
    for (i = 0; i < 30; i++) {
        snprintf(name, sizeof(name), "new%03d with another long name.bin", i);
        check(f_open(&fil, name, FA_CREATE_ALWAYS | FA_WRITE), name);
        write_pat(&fil, 4, i * 64, 64);
        check(f_close(&fil), name);
    }

    /* S8: free a long chain. */
    check(f_unlink("huge.dat"), "unlink huge.dat");

    /* S9: big2.dat = 200000 bytes (tag 5) wraps into the freed chain. */
    create_pat("big2.dat", 5, 200000);
}

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <output-dir>\n", argv[0]);
        return 1;
    }
    const char* out = argv[1];
    static uint8_t work[WORK_SIZE];
    MKFS_PARM opt;

    /* ---- FAT12 pair (au 4096 -> 1024 clusters) ---- */
    g_sectors = NUM_SECTORS16;
    g_mem = calloc(NUM_SECTORS16, SECTOR_SIZE);
    if (!g_mem) { perror("calloc"); return 1; }
    memset(&opt, 0, sizeof(opt));
    opt.fmt = FM_FAT | FM_SFD;
    opt.n_fat = 2;
    opt.au_size = 4096;
    check(f_mkfs("", &opt, work, sizeof(work)), "f_mkfs FAT12");
    dump(out, "golden-fmt12.img", (size_t)NUM_SECTORS16 * SECTOR_SIZE);
    check(f_mount(&fs, "", 1), "f_mount FAT12");
    if (fs.fs_type != FS_FAT12) {
        fprintf(stderr, "expected FAT12, got fs_type=%d\n", fs.fs_type);
        return 1;
    }
    script_small();
    check(f_mount(NULL, "", 0), "unmount FAT12");
    dump(out, "golden-torture12.img", (size_t)NUM_SECTORS16 * SECTOR_SIZE);
    free(g_mem);

    /* ---- FAT16 pair (au 512 -> 8192 clusters) ---- */
    g_sectors = NUM_SECTORS16;
    g_mem = calloc(NUM_SECTORS16, SECTOR_SIZE);
    if (!g_mem) { perror("calloc"); return 1; }
    memset(&opt, 0, sizeof(opt));
    opt.fmt = FM_FAT | FM_SFD;
    opt.n_fat = 2;
    opt.au_size = 512;
    check(f_mkfs("", &opt, work, sizeof(work)), "f_mkfs FAT16");
    dump(out, "golden-fmt16.img", (size_t)NUM_SECTORS16 * SECTOR_SIZE);
    check(f_mount(&fs, "", 1), "f_mount FAT16");
    if (fs.fs_type != FS_FAT16) {
        fprintf(stderr, "expected FAT16, got fs_type=%d\n", fs.fs_type);
        return 1;
    }
    script_small();
    check(f_mount(NULL, "", 0), "unmount FAT16");
    dump(out, "golden-torture16.img", (size_t)NUM_SECTORS16 * SECTOR_SIZE);
    free(g_mem);

    /* ---- FAT32 pair ---- */
    g_sectors = NUM_SECTORS32;
    g_mem = calloc(NUM_SECTORS32, SECTOR_SIZE);
    if (!g_mem) { perror("calloc"); return 1; }
    memset(&opt, 0, sizeof(opt));
    opt.fmt = FM_FAT32 | FM_SFD;
    opt.n_fat = 2;
    opt.au_size = 512;
    check(f_mkfs("", &opt, work, sizeof(work)), "f_mkfs FAT32");
    dump(out, "golden-fmt32.img", (size_t)NUM_SECTORS32 * SECTOR_SIZE);
    check(f_mount(&fs, "", 1), "f_mount FAT32");
    if (fs.fs_type != FS_FAT32) {
        fprintf(stderr, "expected FAT32, got fs_type=%d\n", fs.fs_type);
        return 1;
    }
    script32();
    check(f_mount(NULL, "", 0), "unmount FAT32");
    dump(out, "golden-torture32.img", (size_t)NUM_SECTORS32 * SECTOR_SIZE);
    free(g_mem);

    return 0;
}

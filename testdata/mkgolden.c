// mkgolden.c — deterministic FatFs torture-image generator.
//
// Produces testdata/golden-torture.img, which golden_torture_test.go
// reproduces byte-for-byte with the Go port by replaying the exact same
// operation script using the high-level exported API.
//
// The block device matches the Go test BlockByteSlice: memory starts zeroed,
// writes are persistent, reads return exactly what was written.
//
// Build & run from the fat/ directory (after enabling FF_USE_MKFS in ffconf.h):
//   gcc -o /tmp/mkgolden testdata/mkgolden.c local/ff16/source/ff.c \
//       local/ff16/source/ffunicode.c -Ilocal/ff16/source
//   /tmp/mkgolden testdata/golden-torture.img

// Include FatFs first so that all its types (BYTE, DWORD, LBA_t, etc.) are defined.
#include "ff.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Provide the DSTATUS/DRESULT/RES_* definitions that diskio.h would have provided.
// This avoids pulling in diskio.c (and its platform.h dependency).
typedef BYTE DSTATUS;
typedef enum {
	RES_OK = 0, RES_ERROR, RES_WRPRT, RES_NOTRDY, RES_PARERR
} DRESULT;

#define GET_SECTOR_COUNT	1
#define GET_SECTOR_SIZE		2
#define GET_BLOCK_SIZE		3
#define CTRL_SYNC		0

#define SECTOR_SIZE 512
#define NUM_SECTORS 8192   // 4 MiB image (minimum practical size for FAT32 mkfs)
#define WORK_SIZE   8192

static uint8_t mem[SECTOR_SIZE * NUM_SECTORS];
static FATFS fs;
static FIL fil;

// Block device callbacks for FatFs.
DSTATUS disk_initialize(BYTE pdrv) { (void)pdrv; return 0; }
DSTATUS disk_status(BYTE pdrv)     { (void)pdrv; return 0; }
DRESULT disk_read(BYTE pdrv, BYTE* buf, LBA_t sector, UINT count) {
    (void)pdrv;
    memcpy(buf, &mem[sector * SECTOR_SIZE], count * SECTOR_SIZE);
    return RES_OK;
}
DRESULT disk_write(BYTE pdrv, const BYTE* buf, LBA_t sector, UINT count) {
    (void)pdrv;
    memcpy(&mem[sector * SECTOR_SIZE], buf, count * SECTOR_SIZE);
    return RES_OK;
}
DRESULT disk_ioctl(BYTE pdrv, BYTE cmd, void* buff) {
    (void)pdrv;
    if (cmd == GET_SECTOR_COUNT) { *(DWORD*)buff = NUM_SECTORS; return RES_OK; }
    if (cmd == GET_SECTOR_SIZE)  { *(WORD*)buff  = SECTOR_SIZE;  return RES_OK; }
    if (cmd == GET_BLOCK_SIZE)   { *(DWORD*)buff = 1;            return RES_OK; }
    if (cmd == CTRL_SYNC)        { return RES_OK; }
    return RES_PARERR;
}

// Deterministic data pattern generator (must match Go side).
static uint8_t pat(int tag, int i) {
    return (uint8_t)(i * 31 + tag * 17 + 7);
}

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

static void check(FRESULT fr, const char* what) {
    if (fr != FR_OK) {
        fprintf(stderr, "mkgolden: %s: %d\n", what, fr);
        exit(1);
    }
}

DWORD get_fattime(void) { return 0x12345678; }

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <output.img>\n", argv[0]);
        return 1;
    }
    const char* out = argv[1];

    memset(mem, 0, sizeof(mem));

    // Format the volume using the real FatFs API.
    uint8_t work[WORK_SIZE];
    MKFS_PARM opt = { .fmt = FM_ANY, .n_fat = 2, .align = 0, .n_root = 0, .au_size = 0 };
    check(f_mkfs("", &opt, work, sizeof(work)), "f_mkfs");

    check(f_mount(&fs, "", 1), "f_mount");

    // --- Scripted torture operations (must stay in lockstep with Go test) ---

    // 1. Create "a.dat" and write 3 clusters of patterned data (tag 1).
    check(f_open(&fil, "a.dat", FA_CREATE_ALWAYS | FA_WRITE), "create a.dat");
    write_pat(&fil, 1, 0, 3 * 4096);
    check(f_close(&fil), "close a.dat");

    // 2. Create "b.dat" and write 2 clusters (tag 2).
    check(f_open(&fil, "b.dat", FA_CREATE_ALWAYS | FA_WRITE), "create b.dat");
    write_pat(&fil, 2, 0, 2 * 4096);
    check(f_close(&fil), "close b.dat");

    // 3. Extend "a.dat" by another 2 clusters (forces allocator to pick new clusters).
    check(f_open(&fil, "a.dat", FA_OPEN_EXISTING | FA_WRITE), "open a.dat for extend");
    check(f_lseek(&fil, 3 * 4096), "seek a.dat");
    write_pat(&fil, 1, 3 * 4096, 2 * 4096);
    check(f_close(&fil), "close a.dat after extend");

    // 4. Create a fragmented file "frag.dat" by writing, closing, re-opening and appending.
    check(f_open(&fil, "frag.dat", FA_CREATE_ALWAYS | FA_WRITE), "create frag.dat");
    write_pat(&fil, 3, 0, 4096);
    check(f_close(&fil), "close frag.dat");
    check(f_open(&fil, "frag.dat", FA_OPEN_EXISTING | FA_WRITE), "reopen frag.dat");
    check(f_lseek(&fil, 4096), "seek frag.dat");
    write_pat(&fil, 3, 4096, 4096); // second cluster may land elsewhere
    check(f_close(&fil), "close frag.dat");

    // 5. Create 32 small files to force directory growth.
    for (int i = 0; i < 32; i++) {
        char name[32];
        sprintf(name, "small%02d.txt", i);
        check(f_open(&fil, name, FA_CREATE_ALWAYS | FA_WRITE), name);
        const char* msg = "hello";
        UINT bw;
        f_write(&fil, msg, strlen(msg), &bw);
        check(f_close(&fil), name);
    }

    // 6. Sync and unmount.
    check(f_mount(NULL, "", 0), "unmount");

    // Write the image.
    FILE* f = fopen(out, "wb");
    if (!f) { perror("fopen"); return 1; }
    if (fwrite(mem, 1, sizeof(mem), f) != sizeof(mem)) {
        fprintf(stderr, "short write of image\n");
        return 1;
    }
    fclose(f);
    printf("wrote %s (%zu bytes)\n", out, sizeof(mem));
    return 0;
}

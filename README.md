# fat
[![go.dev reference](https://pkg.go.dev/badge/github.com/soypat/fat)](https://pkg.go.dev/github.com/soypat/fat)
[![Go Report Card](https://goreportcard.com/badge/github.com/soypat/fat)](https://goreportcard.com/report/github.com/soypat/fat)
[![codecov](https://codecov.io/gh/soypat/fat/branch/main/graph/badge.svg)](https://codecov.io/gh/soypat/fat)
[![Go](https://github.com/soypat/fat/actions/workflows/go.yml/badge.svg)](https://github.com/soypat/fat/actions/workflows/go.yml)
[![sourcegraph](https://sourcegraph.com/github.com/soypat/fat/-/badge.svg)](https://sourcegraph.com/github.com/soypat/fat?badge)
[![License: BSD-3Clause](https://img.shields.io/badge/License-BSD-3.svg)](https://opensource.org/licenses/bsd-3-clause)

A File Allocation Table implementation written in Go. Intended for use in embedded systems 
with SD cards, USBs, MMC devices and also usable with an in-RAM representation. Inspired by [FatFs](https://github.com/abbrev/fatfs).

This is a *Work in Progress*.

How to install package with newer versions of Go (+1.16):
```sh
go mod download github.com/soypat/fat@latest
```

### Basic usage example

The following example does the following:
1. Mounts a FAT filesystem to the `fat.FS` type.
2. Creates a new empty file called `newfile.txt`, replacing any existing file.
3. Writes `Hello, World!` to that file.
4. Closes the file to synchronize pending changes to the FAT filesystem.
5. Opens the file in read mode and reads all of it's contents and prints them to standard output.

```go
package main

import "github.com/soypat/fat"

func main() {
	// device could be an SD card, RAM, or anything that implements the BlockDevice interface.
	device := NewFATDevice()
	var fs fat.FS
	err := fs.Mount(device, device.BlockSize(), fat.ModeRW)
	if err != nil {
		panic(err)
	}
	var file fat.File
	err = fs.OpenFile(&file, "newfile.txt", fat.ModeCreateAlways|fat.ModeWrite)
	if err != nil {
		panic(err)
	}

	_, err = file.Write([]byte("Hello, World!"))
	if err != nil {
		panic(err)
	}
	err = file.Close()
	if err != nil {
		panic(err)
	}

	// Read back the file:
	err = fs.OpenFile(&file, "newfile.txt", fat.ModeRead)
	if err != nil {
		panic(err)
	}
	data, err := io.ReadAll(&file)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
	file.Close()
    // Output: Hello, World!
}
```
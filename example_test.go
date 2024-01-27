package fat_test

import (
	"fmt"
	"io"

	"github.com/soypat/fat"
)

func ExampleFS_basic_usage() {
	// device could be an SD card, RAM, or anything that implements the BlockDevice interface.
	device := fat.DefaultFATByteBlocks(32000)
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
	// Output:
	// Hello, World!
}

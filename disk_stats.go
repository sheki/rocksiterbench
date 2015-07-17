package rocksiterbench

import (
	"fmt"
	"log"
	"time"

	"github.com/shirou/gopsutil/disk"
)

func RecordDiskUsage(path string) {
	c := time.Tick(30 * time.Second)
	for now := range c {
		usage, err := disk.DiskUsage(path)
		if err != nil {
			log.Println(err)
		}
		fmt.Printf("disk size %d bytes at %d seconds\n", usage.Used, now.Unix())
	}
}

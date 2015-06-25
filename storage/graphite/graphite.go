package graphite

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"strings"
)

type graphiteStorage struct {
	machineName        string
	dbHostPort         string
	lock               sync.Mutex
	prefix             string
	graphiteConnection net.Conn
}

const (
	colTimestamp          string = "time"
	colMachineName        string = "machine"
	colContainerName      string = "container_name"
	colCpuCumulativeUsage string = "cpu_cumulative_usage"
	// Memory Usage
	colMemoryUsage string = "memory_usage"
	// Working set size
	colMemoryWorkingSet string = "memory_working_set"
	// Cumulative count of bytes received.
	colRxBytes string = "rx_bytes"
	// Cumulative count of receive errors encountered.
	colRxErrors string = "rx_errors"
	// Cumulative count of bytes transmitted.
	colTxBytes string = "tx_bytes"
	// Cumulative count of transmit errors encountered.
	colTxErrors string = "tx_errors"
	// Filesystem device.
	colFsDevice = "fs_device"
	// Filesystem limit.
	colFsLimit = "fs_limit"
	// Filesystem usage.
	colFsUsage = "fs_usage"
)

func convertToUint64(v interface{}) (uint64, error) {
	if v == nil {
		return 0, nil
	}
	switch x := v.(type) {
	case uint64:
		return x, nil
	case int:
		if x < 0 {
			return 0, fmt.Errorf("negative value: %v", x)
		}
		return uint64(x), nil
	case int32:
		if x < 0 {
			return 0, fmt.Errorf("negative value: %v", x)
		}
		return uint64(x), nil
	case int64:
		if x < 0 {
			return 0, fmt.Errorf("negative value: %v", x)
		}
		return uint64(x), nil
	case float64:
		if x < 0 {
			return 0, fmt.Errorf("negative value: %v", x)
		}
		return uint64(x), nil
	case uint32:
		return uint64(x), nil
	}
	return 0, fmt.Errorf("unknown type")
}

func getGraphiteName(refName string) string {
	if refName == "/" {
		return "root"
	}
	return strings.Replace("root"+refName, "/", ".", -1)
}

func (self *graphiteStorage) AddStats(ref info.ContainerReference, 
	stats *info.ContainerStats) error {

	if stats == nil {
		return nil
	}

	var buffer bytes.Buffer
	fmt.Println("Aliases : ", ref.Aliases)
	for _, alias := range ref.Aliases {
		buffer.WriteString(fmt.Sprintf("%s.%s.%s.%s %d %d\n",
			self.prefix,
			self.machineName,
			alias,
			colCpuCumulativeUsage,
			stats.Cpu.Usage.Total,
			stats.Timestamp.Unix()))
		buffer.WriteString(fmt.Sprintf("%s.%s.%s.%s %d %d\n",
			self.prefix,
			self.machineName,
			alias,
			colMemoryUsage,
			stats.Memory.Usage,
			stats.Timestamp.Unix()))
		buffer.WriteString(fmt.Sprintf("%s.%s.%s.%s %d %d\n",
			self.prefix,
			self.machineName,
			alias,
			colMemoryWorkingSet,
			stats.Memory.WorkingSet,
			stats.Timestamp.Unix()))
	}

	fmt.Println(buffer.String())

	err := func() error {
		self.lock.Lock()
		defer self.lock.Unlock()
		writeTimeoutDeadline, _ := time.ParseDuration("1s")
		self.graphiteConnection.SetWriteDeadline(time.Now().Add(writeTimeoutDeadline))
		numBytesWritten, err := self.graphiteConnection.Write(buffer.Bytes())

		fmt.Println("Num bytes written ", buffer.Len(), numBytesWritten)
		if err != nil || numBytesWritten < buffer.Len() {
			fmt.Println("Error writing to graphite1")
			self.graphiteConnection.Close()
			self.graphiteConnection, err = net.DialTimeout("tcp", self.dbHostPort, 5*time.Second)
			numBytesWritten1, err1 := self.graphiteConnection.Write(buffer.Bytes())
			fmt.Println("1. Num bytes written ", buffer.Len(), numBytesWritten1)
			if err1 != nil || numBytesWritten1 < buffer.Len() {
				fmt.Println("Error writing to graphite2")
				return fmt.Errorf("Failed to write to graphite")
			}
		}
		return nil
	}()

	fmt.Println("Send shit to graphite ", stats.Timestamp.Unix())
	return err

}

func (self *graphiteStorage) RecentStats(containerName string, numStats int) ([]*info.ContainerStats, error) {

	fmt.Println("Recent Stats from graphiteeee")
	return nil, nil
}

func (self *graphiteStorage) Close() error {
	fmt.Println("Close graphite")
	return nil
}

func New(host string, 
	dbHostPort string, 
	prefix string,
) (*graphiteStorage, error) {
	graphiteConnection, err := net.DialTimeout("tcp", 
		dbHostPort, 
		5*time.Second,
	)
	if err != nil {
		return nil, err
	}
	ret := &graphiteStorage{
		machineName:        host,
		dbHostPort:         dbHostPort,
		prefix:             prefix,
		graphiteConnection: graphiteConnection,
	}
	return ret, nil
}
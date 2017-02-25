package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/objectserver"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var verbose bool

func logInfo(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func logDebug(format string, args ...interface{}) {
	if verbose {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

func syncFile(c SyncClient, node ring.Device, objFile string, policy int) (err error) {
	lst := strings.Split(objFile, string(os.PathSeparator))
	pathVals := lst[len(lst)-6:]

	fp, err := os.Open(objFile)
	if err != nil {
		return fmt.Errorf("unable to open file (%v): %s", err, objFile)
	}
	defer fp.Close()

	logDebug("opened %s", objFile)

	meta, err := objectserver.ReadMetadata(fp.Fd())
	if err != nil {
		return fmt.Errorf("error reading metadata")
	}

	df, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("error serializing metadata")
	}

	nameVals := strings.SplitN(meta["name"], "/", 4)

	stream, err := c.Sync(
		metadata.NewContext(
			context.Background(),
			metadata.Pairs(
				"df_metadata", string(df),
				"policy_index", strconv.Itoa(policy),
				"device", pathVals[0],
				"partition", pathVals[2],
				"account", nameVals[1],
				"container", nameVals[2],
				"obj", nameVals[3],
			),
		),
	)
	if err != nil {
		return err
	}
	defer stream.CloseSend()
	scratch := make([]byte, 65538)
	l, err := fp.Read(scratch)
	for l > 0 && err == nil {
		if err = stream.Send(&Chunk{Data: scratch[0:l]}); err != nil {
			return fmt.Errorf("chunk send failed: %v", err)
		}
		l, err = fp.Read(scratch)
	}
	if err != nil && err != io.EOF {
		return err
	}
	_, err = stream.CloseAndRecv()
	if err == nil {
		logDebug("successfully replicated %s", objFile)
	}
	return err
}

func lofWalk(objChan chan string, cancel chan struct{}, path string, left []string) {
	if subs, err := filepath.Glob(filepath.Join(path, left[0])); err == nil {
		for _, sub := range subs {
			if len(left) == 1 {
				select {
				case objChan <- sub:
				case <-cancel:
					return
				}
			} else {
				lofWalk(objChan, cancel, sub, left[1:])
			}
		}
	}
}

func listObjFiles(objChan chan string, cancel chan struct{}, objPath string) {
	defer close(objChan)
	lofWalk(objChan, cancel, objPath, []string{"[0-9]*", "[a-f0-9][a-f0-9][a-f0-9]", "????????????????????????????????", "*.[tdm]*"})
}

func replicateDevice(c SyncClient, ring hummingbird.Ring, devicePath string, policy int, threadsPerDevice int) {
	sem := make(chan struct{}, threadsPerDevice)
	objChan := make(chan string, 10*threadsPerDevice)
	cancel := make(chan struct{})
	defer close(cancel)
	go listObjFiles(objChan, cancel, filepath.Join(devicePath, objectserver.PolicyDir(policy)))
	for objFile := range objChan {
		sem <- struct{}{}
		go func() {
			defer func() {
				<-sem
			}()
			lst := strings.Split(objFile, string(os.PathSeparator))
			if part, err := strconv.ParseUint(lst[len(lst)-4], 10, 64); err == nil {
				for _, node := range ring.GetNodes(part) {
					if node.Device == filepath.Base(devicePath) {
						continue
					}
					if err := syncFile(c, node, objFile, policy); err != nil {
						logInfo("error syncing file: %v", err)
					}
				}
			}
		}()
	}
}

func main() {
	var bindIp, deviceRoot string
	var bindPort, policyIndex, threadsPerDevice int
	flag.IntVar(&bindPort, "port", 8091, "")
	flag.IntVar(&policyIndex, "policy-index", 0, "set policy index")
	flag.IntVar(&threadsPerDevice, "threads-per-dev", 4, "set number of threads per device")
	flag.StringVar(&bindIp, "bind-ip", "[::]", "")
	flag.StringVar(&deviceRoot, "devices", "/srv/node", "set devices root")
	flag.BoolVar(&verbose, "verbose", false, "more logging")
	flag.Parse()

	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		panic("Unable to get hash prefix and suffix")
	}

	ring, err := hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix, policyIndex)
	if err != nil {
		panic("Unable to load ring for policy")
	}

	logDebug("connecting...")
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", bindIp, bindPort), grpc.WithInsecure())
	if err != nil {
		panic("Unable to connect")
	}
	defer conn.Close()
	logDebug("connected")

	c := NewSyncClient(conn)

	devices, err := filepath.Glob(filepath.Join(deviceRoot, "*"))
	if err != nil {
		panic("Unable to glob deviceRoot")
	}
	wg := sync.WaitGroup{}
	for _, dev := range devices {
		wg.Add(1)
		go func(dev string) {
			replicateDevice(c, ring, dev, policyIndex, threadsPerDevice)
			wg.Done()
		}(dev)
	}
	wg.Wait()
	logInfo("all workers finished")
}

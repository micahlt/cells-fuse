package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pydio/cells-sdk-go/v5/apiv1"
	apiV1Client "github.com/pydio/cells-sdk-go/v5/apiv1/client"
	"github.com/pydio/cells-sdk-go/v5/apiv1/client/jobs_service"
	"github.com/pydio/cells-sdk-go/v5/apiv1/client/tree_service"
	"github.com/pydio/cells-sdk-go/v5/apiv1/models"
	"github.com/pydio/cells-sdk-go/v5/apiv1/transport/rest"
	sdkS3 "github.com/pydio/cells-sdk-go/v5/apiv1/transport/s3"
	"github.com/winfsp/cgofuse/fuse"
)

const (
	FileSize      = 500 * 1024 * 1024
	CacheTTL      = 5 * time.Second
	ReadAheadSize = 1024 * 1024 * 4 // 4MB chunks for high-latency connections
	PrefetchAhead = 10              // Prefetch 10 chunks ahead (~40MB buffer)
	MaxWorkers    = 20
)

var IgnoredPaths = []string{".hidden", ".Trash", ".Trash-1000", "autorun.inf", ".xdg-volume-info"}

type CacheEntry struct {
	Stat     *fuse.Stat_t
	Node     *models.TreeNode
	Children map[string]*fuse.Stat_t
	ExpireAt time.Time
}

type CellsFuse struct {
	fuse.FileSystemBase
	S3Client        *s3.Client
	metadataCache   sync.Map
	readAheadCache  sync.Map
	prefetchActive  sync.Map // Track which chunks are being prefetched
	workspaceLabels sync.Map
	Logger          func(string, ...interface{})
	*apiV1Client.PydioCellsRestAPI
	// Configurable performance parameters
	readAheadSize int64
	prefetchAhead int64
	cacheChunks   int64
	cacheTTL      time.Duration
}

func createApiClient(session AppSession) *apiV1Client.PydioCellsRestAPI {
	Log(&session, "DEBUG | Creating API client with URL=%s, Token=%s...", session.AppUrl, session.AuthToken[:min(20, len(session.AuthToken))])
	conf := &apiv1.SdkConfig{
		Url:        session.AppUrl,
		SkipVerify: false,
		IdToken:    session.AuthToken,
	}
	rClient, err := rest.GetApiClient(conf, false)
	if err != nil {
		Log(&session, "Error creating API client: %v", err)
		os.Exit(1)
	}
	Log(&session, "DEBUG | API client created successfully")
	return rClient
}

func createS3Client(session AppSession) *s3.Client {
	Log(&session, "DEBUG | Creating S3 client with URL=%s", session.AppUrl)
	// Create a custom resolver to point the S3 client to our specific Pydio Cells URL
	// instead of AWS servers.
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:           session.AppUrl, // Points to the S3 gateway
			SigningRegion: "us-east-1",    // Cells usually defaults to this
		}, nil
	})

	cfg, _ := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(session.AuthToken, "gatewaysecret", "")),
	)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // CRITICAL: Pydio uses /io/bucket-name/path instead of bucket-name.domain
	})
}

func (self *CellsFuse) UpdateClients(session *AppSession) {
	self.S3Client = createS3Client(*session)
	self.PydioCellsRestAPI = createApiClient(*session)
}

// Cache helper methods
func (self *CellsFuse) getCached(path string) (*CacheEntry, bool) {
	if val, ok := self.metadataCache.Load(path); ok {
		entry := val.(*CacheEntry)
		if time.Now().Before(entry.ExpireAt) {
			return entry, true
		}
		self.metadataCache.Delete(path)
	}
	return nil, false
}

func (self *CellsFuse) setCache(path string, entry *CacheEntry) {
	entry.ExpireAt = time.Now().Add(self.cacheTTL)
	self.metadataCache.Store(path, entry)
}

func (self *CellsFuse) invalidateCache(path string) {
	self.metadataCache.Delete(path)
}

func (self *CellsFuse) shouldIgnorePath(path string) bool {
	if slices.Contains(IgnoredPaths, path) {
		self.Logger("Ignoring...")
		return true
	} else if slices.Contains(IgnoredPaths, filepath.Base(path)) {
		self.Logger("Ignoring...")
		return true
	}
	return false
}

func isTempFile(path string) bool {
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".goutputstream") {
		return true
	}
	if strings.HasPrefix(base, ".tmp") || strings.HasSuffix(base, ".tmp") {
		return true
	}
	if strings.HasSuffix(base, ".swp") || strings.HasSuffix(base, ".swx") {
		return true
	}
	if base == ".DS_Store" || strings.HasPrefix(base, "._") {
		return true
	}
	if strings.Contains(base, ".~lock") {
		return true
	}
	return false
}

func (self *CellsFuse) toInternalPath(path string) string {
	if path == "/" || path == "." {
		return path
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 0 {
		return path
	}
	label := parts[0]
	if slug, ok := self.workspaceLabels.Load(label); ok {
		parts[0] = slug.(string)
	}

	for i, part := range parts {
		if part == ".recycle_bin" {
			parts[i] = "recycle_bin"
		}
	}
	return "/" + strings.Join(parts, "/")
}

func (self *CellsFuse) beginOp(op string, path string) (string, int) {
	if self.shouldIgnorePath(path) {
		return "", -fuse.EOPNOTSUPP
	}
	if op != "" {
		self.Logger("FUSE | %s %s", op, path)
	}
	return self.toInternalPath(path), 0
}

func (self *CellsFuse) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	internalPath, errCode := self.beginOp("Getattr", path)
	if errCode != 0 {
		return errCode
	}

	if internalPath == "/" {
		stat.Mode = fuse.S_IFDIR | 0555 // Root directory: Read/Execute only (0555)
		return 0
	}

	// Check cache first
	if cached, ok := self.getCached(internalPath); ok {
		*stat = *cached.Stat
		return 0
	}

	// Create context with timeout for API call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Call Pydio API to get node info
	params := tree_service.NewHeadNodeParams().WithNode(internalPath).WithContext(ctx)
	result, err := self.TreeService.HeadNode(params)
	if err != nil {
		self.Logger("Getattr Error: %v", err)
		return -fuse.ENOENT // File not found
	}

	if result.IsSuccess() {
		node := result.GetPayload().Node

		// Map Pydio node types to FUSE modes
		if *node.Type == models.TreeNodeTypeCOLLECTION {
			stat.Mode = fuse.S_IFDIR | 0755 // Directory
		} else {
			stat.Mode = fuse.S_IFREG | 0644 // Regular file
			if node.MTime != "" {
				mtime, err := strconv.ParseInt(node.MTime, 10, 64)
				if err == nil {
					stat.Mtim = fuse.NewTimespec(time.Unix(mtime, 0))
				} else {
					self.Logger("Error parsing MTime: %v", err)
				}
			}
			if node.Size != "" {
				size, err := strconv.ParseUint(node.Size, 10, 64)
				if err != nil {
					self.Logger("Error formatting size: %v", err)
					return -fuse.ENOENT
				}
				stat.Size = int64(size)
			}
		}
		// Cache the result
		self.setCache(internalPath, &CacheEntry{
			Stat: stat,
			Node: node,
		})
	} else {
		return -fuse.ENOENT
	}
	return 0
}

func (self *CellsFuse) Mkdir(path string, mode uint32) int {
	internalPath, errCode := self.beginOp("Mkdir", path)
	if errCode != 0 {
		return errCode
	}

	pydioPath := strings.TrimPrefix(internalPath, "/")
	params := tree_service.NewCreateNodesParams().WithBody(&models.RestCreateNodesRequest{
		Nodes: []*models.TreeNode{
			{
				Path: pydioPath,
				Type: models.TreeNodeTypeCOLLECTION.Pointer(),
			},
		},
		Recursive: false,
	})
	_, err := self.TreeService.CreateNodes(params)
	if err != nil {
		self.Logger("Mkdir Error for %s: %v\n", path, err)
		if strings.Contains(err.Error(), "403") {
			return -int(fuse.EACCES)
		}
		return -int(fuse.EIO)
	}
	now := time.Now()
	stat := &fuse.Stat_t{
		Mode:  fuse.S_IFDIR | 0755,
		Uid:   uint32(os.Getuid()),
		Gid:   uint32(os.Getgid()),
		Size:  0,
		Ctim:  fuse.NewTimespec(now),
		Mtim:  fuse.NewTimespec(now),
		Atim:  fuse.NewTimespec(now),
		Nlink: 2,
	}
	self.setCache(internalPath, &CacheEntry{
		Stat: stat,
		Node: &models.TreeNode{
			Path: internalPath,
			Type: models.TreeNodeTypeCOLLECTION.Pointer(),
		},
		ExpireAt: time.Now().Add(self.cacheTTL),
	})
	parentDir := filepath.Dir(internalPath)
	self.invalidateCache(parentDir)
	return 0
}

func (self *CellsFuse) Create(path string, flags int, mode uint32) (int, uint64) {
	internalPath, errCode := self.beginOp("Create", path)
	if errCode != 0 {
		return errCode, 0
	}

	// 1. Create a local temporary file to hold the data while it's being written
	tempPath := filepath.Join(os.TempDir(), "cells-"+url.PathEscape(internalPath))
	self.Logger("PYDIO | temp file: " + tempPath)
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		self.Logger("OS | " + err.Error())
		return -int(fuse.EIO), 0
	}
	f.Close()

	// 2. Cache the new file so Getattr can find it immediately
	now := time.Now()
	stat := &fuse.Stat_t{
		Mode:  mode,
		Uid:   uint32(os.Getuid()),
		Gid:   uint32(os.Getgid()),
		Size:  0,
		Ctim:  fuse.NewTimespec(now),
		Mtim:  fuse.NewTimespec(now),
		Atim:  fuse.NewTimespec(now),
		Nlink: 1,
	}

	self.setCache(internalPath, &CacheEntry{
		Stat: stat,
		Node: &models.TreeNode{
			Path: internalPath,
			Type: models.TreeNodeTypeLEAF.Pointer(),
		},
		ExpireAt: time.Now().Add(self.cacheTTL),
	})

	// 3. Invalidate parent so readdir might pick it up if cached
	self.invalidateCache(filepath.Dir(internalPath))

	return 0, 0
}

func (self *CellsFuse) Write(path string, buff []byte, ofst int64, fh uint64) int {
	internalPath, errCode := self.beginOp("Write", path)
	if errCode != 0 {
		return errCode
	}
	tempPath := filepath.Join(os.TempDir(), "cells-"+url.PathEscape(internalPath))

	// Open the local buffer with O_CREATE to treat missing temp file as new file
	f, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		self.Logger("OS | " + err.Error())
		return -int(fuse.EIO)
	}
	defer f.Close()

	// Write to the specific offset requested by the OS
	n, err := f.WriteAt(buff, ofst)
	if err != nil {
		return -int(fuse.EIO)
	}

	return n
}

func (self *CellsFuse) clearReadCache(path string) {
	// iterate and delete any cache keys belonging to this path
	prefix := path + "\x00"
	self.readAheadCache.Range(func(key, value interface{}) bool {
		if k, ok := key.(string); ok {
			if strings.HasPrefix(k, prefix) {
				self.readAheadCache.Delete(key)
			}
		}
		return true
	})
}

func (self *CellsFuse) uploadLocalToPydio(localPath string, pydioPath string) (os.FileInfo, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	pydioKey := strings.TrimPrefix(pydioPath, "/")
	partSize, err := sdkS3.ComputePartSize(fileStat.Size(), int64(50), int64(5000))
	if err != nil {
		return nil, err
	}
	numParts := int(math.Ceil(float64(fileStat.Size()) / float64(partSize)))

	uploader := manager.NewUploader(self.S3Client, func(u *manager.Uploader) {
		u.Concurrency = 4
		u.PartSize = partSize
		u.BufferProvider = sdkS3.NewCallbackTransferProvider(pydioPath, fileStat.Size(), partSize, numParts, true)
	})

	output, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String("io"),
		Key:    aws.String(pydioKey),
		Body:   file,
	})

	if err != nil {
		return nil, err
	}
	self.Logger("ETAG | " + *output.ETag)

	return fileStat, nil
}

func (self *CellsFuse) Release(path string, fh uint64) int {
	internalPath, errCode := self.beginOp("Release", path)
	if errCode != 0 {
		return errCode
	}

	tempPath := filepath.Join(os.TempDir(), "cells-"+url.PathEscape(internalPath))

	if isTempFile(internalPath) {
		// Just clean up the local disk and return
		// os.Remove(tempPath) // DO NOT DELETE: Rename execution might need this file
		return 0
	}

	// Check if file exists before trying upload
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		return 0
	}

	fileStat, err := self.uploadLocalToPydio(tempPath, internalPath)
	if err != nil {
		self.Logger(fmt.Sprintf("Upload failed: %v\n", err))
		return -int(fuse.EIO)
	}

	// Update cache with the new file details so subsequent Getattr calls succeed
	newStat := &fuse.Stat_t{
		Mode: fuse.S_IFREG | 0644,
		Size: fileStat.Size(),
		Mtim: fuse.NewTimespec(fileStat.ModTime()),
		Ctim: fuse.NewTimespec(fileStat.ModTime()),
	}

	self.setCache(internalPath, &CacheEntry{
		Stat: newStat,
		Node: &models.TreeNode{
			Path:  internalPath,
			Type:  models.TreeNodeTypeLEAF.Pointer(),
			Size:  strconv.FormatInt(fileStat.Size(), 10),
			MTime: strconv.FormatInt(fileStat.ModTime().Unix(), 10),
		},
		ExpireAt: time.Now().Add(self.cacheTTL),
	})

	// Invalidate parent directory to ensure it shows up in future listings
	self.invalidateCache(filepath.Dir(internalPath))
	self.clearReadCache(internalPath)

	// Cleanup temp file
	os.Remove(tempPath)

	return 0
}

func BuildRenameParams(source []string, targetFolder string, targetParent bool) string {
	type p struct {
		Target       string   `json:"target"`
		Nodes        []string `json:"nodes"`
		TargetParent bool     `json:"targetParent"`
	}
	i := &p{
		Nodes:        source,
		Target:       targetFolder,
		TargetParent: targetParent,
	}
	data, _ := json.Marshal(i)
	return string(data)
}

func (self *CellsFuse) Rename(oldpath string, newpath string) int {
	internalOld, errCode := self.beginOp("", oldpath)
	if errCode != 0 {
		return errCode
	}
	internalNew, errCode := self.beginOp("", newpath)
	if errCode != 0 {
		return errCode
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if isTempFile(internalOld) {
		// Check for local file presence. If found, we treat it as a local-to-remote upload (atomic rename)
		localPath := filepath.Join(os.TempDir(), "cells-"+url.PathEscape(internalOld))
		if _, err := os.Stat(localPath); err == nil {
			self.Logger(fmt.Sprintf("Rename: Uploading local temp file %s to %s\n", oldpath, newpath))
			fileStat, err := self.uploadLocalToPydio(localPath, internalNew)
			if err != nil {
				return -int(fuse.EIO)
			}
			// Update cache for the new path
			newStat := &fuse.Stat_t{
				Mode: fuse.S_IFREG | 0644,
				Size: fileStat.Size(),
				Mtim: fuse.NewTimespec(fileStat.ModTime()),
				Ctim: fuse.NewTimespec(fileStat.ModTime()),
			}
			self.setCache(internalNew, &CacheEntry{
				Stat: newStat,
				Node: &models.TreeNode{
					Path:  internalNew,
					Type:  models.TreeNodeTypeLEAF.Pointer(),
					Size:  strconv.FormatInt(fileStat.Size(), 10),
					MTime: strconv.FormatInt(fileStat.ModTime().Unix(), 10),
				},
				ExpireAt: time.Now().Add(CacheTTL),
			})
			os.Remove(localPath)
			self.invalidateCache(filepath.Dir(internalNew))
			return 0
		}
	}

	// 1. Prepare Pydio Paths
	oldP := strings.TrimPrefix(internalOld, "/")
	newP := strings.TrimPrefix(internalNew, "/")

	// --- GEDIT COMPATIBILITY STEP ---
	// Check if the target already exists (Nautilus/Gedit atomic save)
	statParams := tree_service.NewBulkStatNodesParams().WithBody(&models.RestGetBulkMetaRequest{
		NodePaths: []string{newP},
	})

	// Note: self.Auth is required here for the REST call
	statResp, err := self.TreeService.BulkStatNodes(statParams)

	// If the file exists, we delete it so the rename can "overwrite" it
	if err == nil && len(statResp.Payload.Nodes) > 0 {
		self.Logger(fmt.Sprintf("Rename: target %s exists, deleting for atomic replace\n", newpath))
		if res := self.Unlink(internalNew); res != 0 {
			return res
		}
	}
	// --------------------------------

	// 2. Build JSON parameters for the Move Job
	targetDir := filepath.Dir(newP)

	type moveParams struct {
		Nodes        []string `json:"nodes"`
		Target       string   `json:"target"`
		TargetParent bool     `json:"targetParent"`
	}

	p := &moveParams{
		Nodes:        []string{oldP},
		Target:       targetDir,
		TargetParent: true,
	}

	jsonBytes, _ := json.Marshal(p)

	// 3. Trigger the Move Job
	jobParams := jobs_service.NewUserCreateJobParams().
		WithJobName("move").
		WithBody(&models.RestUserJobRequest{
			JSONParameters: string(jsonBytes),
		})

	// Ensure your JobsService is initialized and passing Auth
	jobResp, err := self.JobsService.UserCreateJob(jobParams)
	if err != nil {
		self.Logger(fmt.Sprintf("Failed to create move job: %v\n", err))
		return -int(fuse.EIO)
	}

	jobID := jobResp.Payload.JobUUID
	self.Logger(fmt.Sprintf("Move Job ID: %s started for %s -> %s\n", jobID, oldpath, newpath))

	// 4. Poll for Completion
	err = self.pollJobStatus(ctx, jobID)
	if err != nil {
		self.Logger(fmt.Sprintf("Move job %s failed: %v\n", jobID, err))
		return -int(fuse.EIO)
	}

	// 5. Success - Invalidate caches
	self.readAheadCache.Delete(internalOld)
	self.readAheadCache.Delete(internalNew)

	return 0
}

func (self *CellsFuse) Copy(oldpath string, newpath string) int {
	internalOld, errCode := self.beginOp("", oldpath)
	if errCode != 0 {
		return errCode
	}
	internalNew, errCode := self.beginOp("", newpath)
	if errCode != 0 {
		return errCode
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // Copies take longer
	defer cancel()

	oldP := strings.TrimPrefix(internalOld, "/")
	newP := strings.TrimPrefix(internalNew, "/")
	targetDir := filepath.Dir(newP)

	// 1. Build JSON parameters for the 'copy' job
	type copyParams struct {
		Nodes        []string `json:"nodes"`
		Target       string   `json:"target"`
		TargetParent bool     `json:"targetParent"`
	}

	p := &copyParams{
		Nodes:        []string{oldP},
		Target:       targetDir,
		TargetParent: true,
	}
	jsonBytes, _ := json.Marshal(p)

	// 2. Trigger the Copy Job
	jobParams := jobs_service.NewUserCreateJobParams().
		WithJobName("copy").
		WithBody(&models.RestUserJobRequest{
			JSONParameters: string(jsonBytes),
		})

	jobResp, err := self.JobsService.UserCreateJob(jobParams)
	if err != nil {
		self.Logger(fmt.Sprintf("Failed to create copy job: %v\n", err))
		return -int(fuse.EIO)
	}

	// 3. Poll for completion (Reuse the pollJobStatus function from Rename)
	err = self.pollJobStatus(ctx, jobResp.Payload.JobUUID)
	if err != nil {
		return -int(fuse.EIO)
	}

	// 4. Success - Clear cache for the NEW path only
	self.readAheadCache.Delete(internalNew)

	return 0
}

func (self *CellsFuse) pollJobStatus(ctx context.Context, jobID string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Continue
		}

		params := jobs_service.NewUserListJobsParams().WithBody(&models.JobsListJobsRequest{
			JobIDs:    []string{jobID},
			LoadTasks: models.JobsTaskStatusAny.Pointer(),
		})

		resp, err := self.JobsService.UserListJobs(params)
		if err != nil {
			return err
		}

		if len(resp.Payload.Jobs) == 0 {
			// Job not found yet, wait and retry
			continue
		}

		job := resp.Payload.Jobs[0]
		if len(job.Tasks) == 0 {
			// Tasks not spawned yet, wait and retry
			continue
		}

		// Check the status of the tasks
		// In official client, they loop over all tasks, but typically there is one active task chain
		// We look for any failure or completion
		allFinished := true
		for _, task := range job.Tasks {
			status := *task.Status

			switch status {
			case models.JobsTaskStatusError:
				return fmt.Errorf("job failed: %s", task.StatusMessage)
			case models.JobsTaskStatusInterrupted:
				return fmt.Errorf("job interrupted")
			case models.JobsTaskStatusFinished:
				// This task is done
			default:
				// Running, Paused, Queued, Idle, etc.
				allFinished = false
			}
		}

		if allFinished {
			return nil
		}
	}
}

func (self *CellsFuse) Fsync(path string, datasync bool, fh uint64) int {
	internalPath, errCode := self.beginOp("Fsync", path)
	if errCode != 0 {
		return errCode
	}

	// 1. Locate the local buffer in /tmp
	tempPath := filepath.Join(os.TempDir(), "cells-"+url.PathEscape(internalPath))

	// Check if we even have anything to sync
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		return 0 // Nothing locally changed, nothing to sync
	}

	// 2. Perform an immediate S3 upload
	_, err := self.uploadLocalToPydio(tempPath, internalPath)
	if err != nil {
		self.Logger(fmt.Sprintf("Fsync Upload Failed for %s: %v\n", path, err))
		return -int(fuse.EIO)
	}

	return 0
}

func (self *CellsFuse) Unlink(path string) int {
	internalPath, errCode := self.beginOp("Unlink", path)
	if errCode != 0 {
		return errCode
	}
	// 1. Translate path (Remove leading slash)
	pydioPath := strings.TrimPrefix(internalPath, "/")

	// 2. Prepare the Delete request
	// Note: Pydio DeleteNodes takes a list of nodes to delete
	params := tree_service.NewDeleteNodesParams().WithBody(&models.RestDeleteNodesRequest{
		Nodes: []*models.TreeNode{
			{Path: pydioPath},
		},
	})

	// 3. Execute the deletion via REST API
	_, err := self.TreeService.DeleteNodes(params)
	if err != nil {
		self.Logger(fmt.Sprintf("Delete Error for %s: %v\n", path, err))
		return -int(fuse.EIO)
	}

	// 4. Important: Clear your local metadata/readdir cache here
	// If you don't, the file might still show up in 'ls' until the cache expires.
	self.invalidateCache(internalPath)
	self.clearReadCache(internalPath)

	return 0
}

func (self *CellsFuse) Rmdir(path string) int {
	// Just wrap Unlink since Pydio handles both the same way
	return self.Unlink(path)
}

func (self *CellsFuse) Open(path string, flags int) (int, uint64) {
	_, errCode := self.beginOp("Open", path)
	if errCode != 0 {
		return errCode, 0
	}

	// If opening for writing, we might want to ensure the temp file exists,
	// but usually Write/Truncate handles the actual file creation.
	// Returning 0 indicates success.
	return 0, 0
}

func (self *CellsFuse) Truncate(path string, size int64, fh uint64) int {
	internalPath, errCode := self.beginOp("Truncate", path)
	if errCode != 0 {
		return errCode
	}
	tempPath := filepath.Join(os.TempDir(), "cells-"+url.PathEscape(internalPath))

	// Truncate the local temp file to the desired size.
	// If it doesn't exist, we create it (if size is 0, or extend it).
	// 'os.Truncate' requires the file to exist, so we might need OpenFile.
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		self.Logger("OS | Truncate Open Error: " + err.Error())
		return -int(fuse.EIO)
	}
	defer f.Close()

	if err := f.Truncate(size); err != nil {
		self.Logger("OS | Truncate Error: " + err.Error())
		return -int(fuse.EIO)
	}

	// Update the cache size immediately so invalidation/stat works
	if entry, ok := self.getCached(internalPath); ok {
		entry.Stat.Size = size
		self.setCache(internalPath, entry)
	}
	self.clearReadCache(internalPath)

	return 0
}

func (self *CellsFuse) Utimens(path string, tmsp []fuse.Timespec) int {
	internalPath, errCode := self.beginOp("Utimens", path)
	if errCode != 0 {
		return errCode
	}
	// Update the local cache with the new times so 'ls -l' shows changes immediately
	if entry, ok := self.getCached(internalPath); ok {
		if len(tmsp) > 0 {
			entry.Stat.Atim = tmsp[0]
		}
		if len(tmsp) > 1 {
			entry.Stat.Mtim = tmsp[1]
		}
		self.setCache(internalPath, entry)
	}
	// We return 0 (success) effectively "swallowing" the time update for the remote server
	// unless we want to send a metdata update API call to Pydio.
	return 0
}

func (self *CellsFuse) Chmod(path string, mode uint32) int {
	_, errCode := self.beginOp("Chmod", path)
	if errCode != 0 {
		return errCode
	}
	// Pydio permissions are handled by ACLs, not unix modes.
	// We swallow this to appease editors like nano.
	return 0
}

func (self *CellsFuse) Chown(path string, uid uint32, gid uint32) int {
	_, errCode := self.beginOp("Chown", path)
	if errCode != 0 {
		return errCode
	}
	// Pydio does not support changing ownership via unix UID/GID.
	return 0
}

func (self *CellsFuse) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	if self.shouldIgnorePath(path) {
		return -fuse.EOPNOTSUPP
	}
	self.Logger("FUSE | Readdir")
	fill(".", nil, 0)
	fill("..", nil, 0)

	internalPath := self.toInternalPath(path)

	// Check cache first
	if cached, ok := self.getCached(internalPath); ok && cached.Children != nil {
		for name, stat := range cached.Children {
			if !fill(name, stat, 0) {
				break
			}
		}
		return 0
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	params := tree_service.NewBulkStatNodesParams().WithBody(&models.RestGetBulkMetaRequest{
		NodePaths: []string{internalPath + "/*"},
	}).WithContext(ctx)

	result, err := self.TreeService.BulkStatNodes(params)
	if err != nil {
		self.Logger(fmt.Sprintf("DEBUG Readdir: BulkStatNodes error for path %s: %v\n", path, err.Error()))
		return -fuse.EIO
	}

	// Check if the response contains an error
	if result.Payload != nil {
		self.Logger(fmt.Sprintf("DEBUG Readdir: Got %d nodes for path %s\n", len(result.Payload.Nodes), path))
	} else {
		self.Logger(fmt.Sprintf("DEBUG Readdir: Payload is nil for path %s\n", path))
	}

	// We use a local map to collect results before updating the main cache
	children := make(map[string]*fuse.Stat_t)
	var mu sync.Mutex                            // Protects the 'children' map during concurrent writes
	var wg sync.WaitGroup                        // Waits for all goroutines to finish
	semaphore := make(chan struct{}, MaxWorkers) // Limits concurrency to MaxWorkers

	for _, node := range result.Payload.Nodes {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire token (blocks if full)
		go func(n *models.TreeNode) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release token

			name := filepath.Base(n.Path)
			if path == "/" && n.MetaStore != nil {
				// Display workspaces by label, not slug
				if label, ok := n.MetaStore["ws_label"]; ok && label != "" {
					name = strings.Trim(label, "\"")
					self.workspaceLabels.Store(name, filepath.Base(n.Path))
				}
			}

			if name == "recycle_bin" {
				name = ".recycle_bin"
			}

			stat := &fuse.Stat_t{}

			if *n.Type == models.TreeNodeTypeCOLLECTION {
				stat.Mode = fuse.S_IFDIR | 0755
			} else {
				stat.Mode = fuse.S_IFREG | 0644
				if n.Size != "" {
					size, err := strconv.ParseUint(n.Size, 10, 64)
					if err == nil {
						stat.Size = int64(size)
					}
				}
				if node.MTime != "" {
					mtime, err := strconv.ParseInt(node.MTime, 10, 64)
					if err == nil {
						stat.Mtim = fuse.NewTimespec(time.Unix(mtime, 0))
					} else {
						self.Logger(err.Error())
					}
				}
			}

			// Cache individual node metadata so subsequent GetAttr calls are fast
			self.setCache(n.Path, &CacheEntry{
				Stat: stat,
				Node: n,
			})

			mu.Lock()
			children[name] = stat
			mu.Unlock()
		}(node)
	}

	wg.Wait()

	// Cache directory contents
	self.setCache(internalPath, &CacheEntry{
		Children: children,
		Stat:     &fuse.Stat_t{Mode: fuse.S_IFDIR | 0755},
	})

	// Fill the directory
	for name, stat := range children {
		if !fill(name, stat, 0) {
			break
		}
	}
	return 0
}

func (self *CellsFuse) Read(path string, buff []byte, ofst int64, fh uint64) int {
	self.Logger("READ: path=%s offset=%d size=%d", path, ofst, len(buff))
	internalPath, errCode := self.beginOp("", path)
	if errCode != 0 {
		return errCode
	}

	startTime := time.Now()
	totalRead := 0

	// Loop until the requested buffer is full or we hit EOF
	for totalRead < len(buff) {
		currentOffset := ofst + int64(totalRead)
		chunkIndex := currentOffset / self.readAheadSize
		chunkOffset := chunkIndex * self.readAheadSize
		// Use null byte as separator to avoid collisions with filenames
		cacheKey := fmt.Sprintf("%s\x00%d", internalPath, chunkIndex)

		var data []byte
		if val, ok := self.readAheadCache.Load(cacheKey); ok {
			data = val.([]byte)
			self.Logger("CACHE HIT: chunk=%d", chunkIndex)
			// Trigger prefetch even on cache hits to stay ahead
			for i := int64(1); i <= self.prefetchAhead; i++ {
				go self.prefetchChunk(internalPath, chunkIndex+i)
			}
		} else {
			self.Logger("CACHE MISS: chunk=%d, fetching from S3...", chunkIndex)
			fetchStart := time.Now()

			// Fetch chunk from S3 using dynamically sized buffer
			data = make([]byte, self.readAheadSize)

			// Explicitly manage context cancelation in the loop
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			// Range is inclusive
			byteRange := fmt.Sprintf("bytes=%d-%d", chunkOffset, chunkOffset+int64(len(data))-1)
			input := &s3.GetObjectInput{
				Bucket: aws.String("io"),
				Key:    aws.String(internalPath),
				Range:  aws.String(byteRange),
			}

			output, err := self.S3Client.GetObject(ctx, input)
			if err != nil {
				cancel()
				// If 416 (Range Not Satisfiable), it usually means we are trying to read past EOF.
				if strings.Contains(err.Error(), "416") {
					self.Logger("EOF reached (416 error)")
					break
				}
				self.Logger("S3 Read Error at offset=%d chunk=%d: %v", currentOffset, chunkIndex, err)
				break
			}

			n, err := io.ReadFull(output.Body, data)
			output.Body.Close()
			cancel()

			fetchDuration := time.Since(fetchStart)
			self.Logger("S3 FETCH: chunk=%d bytes=%d duration=%v", chunkIndex, n, fetchDuration)

			if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
				self.Logger(fmt.Sprintf("Body Read Error: %v\n", err))
				break
			}

			// Resize slice to actual read count
			data = data[:n]
			self.readAheadCache.Store(cacheKey, data)

			// Aggressively prefetch multiple chunks ahead for high-latency connections
			for i := int64(1); i <= self.prefetchAhead; i++ {
				go self.prefetchChunk(internalPath, chunkIndex+i)
			}
		}

		// Keep configured number of chunks cached
		if chunkIndex > self.cacheChunks {
			self.readAheadCache.Delete(fmt.Sprintf("%s\x00%d", internalPath, chunkIndex-self.cacheChunks))
		}

		relativeOffset := currentOffset - chunkOffset
		if relativeOffset >= int64(len(data)) {
			// We wanted to read from this chunk, but the offset is beyond the data we got.
			// This implies EOF.
			break
		}

		available := int64(len(data)) - relativeOffset
		remaining := int64(len(buff) - totalRead)
		toCopy := available
		if toCopy > remaining {
			toCopy = remaining
		}

		copy(buff[totalRead:], data[relativeOffset:relativeOffset+toCopy])
		totalRead += int(toCopy)

		// If this chunk was smaller than expected, it's the last chunk.
		if len(data) < int(self.readAheadSize) {
			break
		}
	}

	duration := time.Since(startTime)
	self.Logger("READ COMPLETE: path=%s offset=%d requested=%d actual=%d duration=%v",
		path, ofst, len(buff), totalRead, duration)
	return totalRead
}

func (self *CellsFuse) prefetchChunk(path string, chunkIndex int64) {
	cacheKey := fmt.Sprintf("%s\x00%d", path, chunkIndex)

	// Skip if already cached
	if _, exists := self.readAheadCache.Load(cacheKey); exists {
		return
	}

	// Skip if already being prefetched
	if _, active := self.prefetchActive.LoadOrStore(cacheKey, true); active {
		return
	}
	defer self.prefetchActive.Delete(cacheKey)

	data := make([]byte, self.readAheadSize)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	chunkOffset := chunkIndex * self.readAheadSize
	byteRange := fmt.Sprintf("bytes=%d-%d", chunkOffset, chunkOffset+int64(len(data))-1)

	output, err := self.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("io"),
		Key:    aws.String(path),
		Range:  aws.String(byteRange),
	})

	if err != nil {
		return
	}

	n, err := io.ReadFull(output.Body, data)
	output.Body.Close()

	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return
	}

	data = data[:n]
	self.readAheadCache.Store(cacheKey, data)
	self.Logger("PREFETCH: chunk=%d bytes=%d", chunkIndex, n)
}

func expandPath(path string) string {
	if strings.HasPrefix(path, "~/") {
		dirname, _ := os.UserHomeDir()
		return filepath.Join(dirname, path[2:])
	}
	return path
}

func runFuseBackground(session *AppSession, mountSignal chan bool) {
	// A sub-channel to catch when the FUSE mount naturally exits (e.g. manual unmount)
	done := make(chan bool, 1)

	for {
		select {
		case shouldMount := <-mountSignal:
			if shouldMount && session.FuseHost == nil {
				Log(session, "Mounting Pydio Cells...")
				Log(session, "DEBUG | Using AppUrl=%s", session.AppUrl)
				Log(session, "DEBUG | Token length=%d", len(session.AuthToken))
				Log(session, "DEBUG | Full token: %s", session.AuthToken)

				// Initialize your fuse implementation
				cf := &CellsFuse{
					S3Client:          createS3Client(*session),
					PydioCellsRestAPI: createApiClient(*session),
					Logger: func(format string, args ...interface{}) {
						Log(session, format, args...)
					},
					readAheadSize: int64(session.ChunkSizeMB) * 1024 * 1024,
					prefetchAhead: int64(session.PrefetchChunks),
					cacheChunks:   int64(session.CacheChunks),
					cacheTTL:      time.Duration(session.CacheTTLSeconds) * time.Second,
				}
				session.CellsFuse = cf
				session.FuseHost = fuse.NewFileSystemHost(cf)

				go func() {
					finalMountPoint := expandPath(session.MountPoint)
					success := session.FuseHost.Mount(finalMountPoint, []string{})
					if !success {
						Log(session, "FUSE Mount failed.")
						session.IsMounted = false
						select {
						case session.MountErrorChannel <- "Failed to mount FUSE filesystem. Check that the mount point exists and is not already in use.":
						default:
						}
						select {
						case session.TrayUpdateSignal <- true:
						default:
						}
					}
					done <- true
				}()

				session.IsMounted = true
				select {
				case session.TrayUpdateSignal <- true:
				default:
				}

			} else if !shouldMount {
				Log(session, "Unmounting by GUI request...")
				if session.FuseHost != nil {
					Log(session, "Calling Unmount()...")
					result := session.FuseHost.Unmount()
					Log(session, "Unmount returned: %v", result)
					if !result {
						Log(session, "Unmount failed, filesystem may still be busy")
					}
				}
				session.IsMounted = false
				select {
				case session.TrayUpdateSignal <- true:
				default:
				}
			}

		case <-done:
			Log(session, "FUSE process exited.")
			session.IsMounted = false
			session.FuseHost = nil
			select {
			case session.TrayUpdateSignal <- true:
			default:
			}
		}
	}
}

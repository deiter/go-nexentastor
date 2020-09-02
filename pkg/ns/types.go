package ns

import (
	"strings"
	"time"
)

// ACLRuleSet - filesystem ACL rule set
type ACLRuleSet int64

const (
	// ACLReadOnly - apply read only set of rules to filesystem
	ACLReadOnly ACLRuleSet = iota

	// ACLReadWrite - apply full access set of rules to filesystem
	ACLReadWrite
)

// License - NexentaStor license
type License struct {
	Valid   bool   `json:"valid"`
	Expires string `json:"expires"`
}

// Filesystem - NexentaStor filesystem
type Filesystem struct {
	Path           string `json:"path"`
	MountPoint     string `json:"mountPoint"`
	SharedOverNfs  bool   `json:"sharedOverNfs"`
	SharedOverSmb  bool   `json:"sharedOverSmb"`
	BytesAvailable int64  `json:"bytesAvailable"`
	BytesUsed      int64  `json:"bytesUsed"`
	QuotaSize      int64  `json:"quotaSize"`
}

type Project struct {
	Pool string `json:"poolName"`
	Name string `json:"projectName"`
}

// Share - InteliFlash share
type Share_v1 struct {
	PoolName      string `json:"poolName"`
	ProjectName   string `json:"projectName"`
	Name          string `json:"name"`
	Path          string `json:"datasetPath,omitempty"`
	MountPoint    string `json:"mountpoint,omitempty"`
	AvailableSize int64  `json:"availableSize,omitempty"`
	TotalSize     int64  `json:"totalSize,omitempty"`
	Local         bool   `json:"local,omitempty"`
}

type Share_v2 struct {
	PoolName      string `json:"poolName"`
	ProjectName   string `json:"projectName"`
	Name          string `json:"name"`
	Path          string `json:"zfsDataSetName"`
	MountPoint    string `json:"mountPoint"`
	ShareName     string `json:"cifsDisplayName"`
	ShareNfs      string `json:"sharenfs"`
	ShareSmb      string `json:"sharesmb"`
	AvailableSize int64  `json:"availableSize"`
	TotalSize     int64  `json:"totalSize"`
	QuotaSize     int64  `json:"quotaInByte"`
}

// Volume - NexentaStor volume
type Volume struct {
	Path           string `json:"path"`
	BytesAvailable int64  `json:"bytesAvailable"`
	BytesUsed      int64  `json:"bytesUsed"`
	VolumeSize     int64  `json:"volumeSize"`
}

// VolumeGroup - NexentaStor volumeGroup
type VolumeGroup struct {
	Path           string `json:"path"`
	BytesAvailable int64  `json:"bytesAvailable"`
	BytesUsed      int64  `json:"bytesUsed"`
}

// LunMapping - NexentaStor lunmapping
type LunMapping struct {
	Id          string `json:"id"`
	Volume      string `json:"volume"`
	TargetGroup string `json:"targetGroup"`
	HostGroup   string `json:"hostGroup"`
	Lun         int    `json:"lun"`
}

func (fs *Filesystem) String() string {
	return fs.Path
}

// GetDefaultSmbShareName - get default SMB share name (all slashes get replaced by underscore)
// Converts '/pool/dataset/fs' to 'pool_dataset_fs'
func (fs *Filesystem) GetDefaultSmbShareName() string {
	return strings.Replace(strings.TrimPrefix(fs.Path, "/"), "/", "_", -1)
}

// Snapshot - NexentaStor snapshot
type Snapshot struct {
	Path         string    `json:"path"`
	Name         string    `json:"name"`
	Parent       string    `json:"parent"`
	Clones       []string  `json:"clones"`
	CreationTxg  string    `json:"creationTxg"`
	CreationTime time.Time `json:"creationTime"`
}

func (snapshot *Snapshot) String() string {
	return snapshot.Path
}

// RSFCluster - RSF cluster with a name
type RSFCluster struct {
	Name string `json:"clusterName"`
}

// Pool - NS pool
type Pool struct {
	Name string `json:"poolName"`
}

// NEF request/response types

type nefAuthLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type nefAuthLoginResponse struct {
	Token string `json:"token"`
}

type nefStoragePoolsResponse struct {
	Data []Pool `json:"data"`
}

type nefStorageFilesystemsResponse struct {
	Data []Filesystem `json:"data"`
}

type nefStorageVolumesResponse struct {
	Data []Volume `json:"data"`
}

type nefStorageVolumeGroupsResponse struct {
	Data []VolumeGroup `json:"data"`
}

type nefLunMappingsResponse struct {
	Data []LunMapping `json:"data"`
}

type nefStorageSnapshotsResponse struct {
	Data []Snapshot `json:"data"`
}

type nefNasNfsRequest struct {
	Filesystem       string                            `json:"filesystem"`
	Anon             string                            `json:"anon"`
	SecurityContexts []nefNasNfsRequestSecurityContext `json:"securityContexts"`
}
type nefNasNfsRequestSecurityContext struct {
	SecurityModes []string      `json:"securityModes"`
	ReadWriteList []NfsRuleList `json:"readWriteList"`
	ReadOnlyList  []NfsRuleList `json:"readOnlyList"`
}

type NfsRuleList struct {
	Etype  string `json:"etype"`
	Entity string `json:"entity"`
	Mask   int    `json:"mask"`
}

type Portal struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type nefNasSmbResponse struct {
	ShareName string `json:"shareName"`
}

type nefStorageFilesystemsACLRequest struct {
	Type        string   `json:"type"`
	Principal   string   `json:"principal"`
	Flags       []string `json:"flags"`
	Permissions []string `json:"permissions"`
}

type nefRsfClustersResponse struct {
	Data []RSFCluster `json:"data"`
}

type nefJobStatusResponse struct {
	Links []nefJobStatusResponseLink `json:"links"`
}
type nefJobStatusResponseLink struct {
	Rel  string `json:"rel"`
	Href string `json:"href"`
}

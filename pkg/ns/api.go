package ns

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// NexentaStor filesystem list limit (<=)
// TODO change this limit base on specified NS version
const nsFilesystemListLimit = 100

const ZebiSnapshotPrefix = "Manual-S-"

func (p *Provider) Share_v1toFilesystem(share Share_v1) (filesystem Filesystem) {
	filesystem.Path = share.Path
	filesystem.MountPoint = share.MountPoint
	filesystem.BytesAvailable = share.AvailableSize
	filesystem.BytesUsed = share.TotalSize
	filesystem.SharedOverNfs = false
	filesystem.SharedOverSmb = false
	return filesystem
}

func (p *Provider) Share_v2toFilesystem(share Share_v2) (filesystem Filesystem) {
	filesystem.Path = share.Path
	filesystem.MountPoint = share.MountPoint
	filesystem.BytesAvailable = share.AvailableSize
	filesystem.BytesUsed = share.TotalSize
	filesystem.QuotaSize = share.QuotaSize

	if share.ShareNfs != "off" {
		filesystem.SharedOverNfs = true
	} else {
		filesystem.SharedOverNfs = false
	}

	if share.ShareSmb != "off" {
		filesystem.SharedOverSmb = true
	} else {
		filesystem.SharedOverSmb = false
	}

	return filesystem
}

type ZebiPool struct {
	Name          string `json:"name"`
	AvailableSize int64  `json:"availableSize"`
	TotalSize     int64  `json:"totalSize"`
}

func (p *Provider) GetPools() ([]Pool, error) {
	zebiPools := []ZebiPool{}
	err := p.sendRequestWithStruct("listPools", nil, &zebiPools)
	if err != nil {
		return nil, err
	}

	pools := []Pool{}
	for _, zebiPool := range zebiPools {
		pool := Pool{
			Name: zebiPool.Name,
		}
		pools = append(pools, pool)
	}

	return pools, nil
}

type GetProjectParameters struct {
	Pool  string
	Name  string
	Local bool
}

func (p GetProjectParameters) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Pool, p.Name, p.Local}
	return json.Marshal(list)
}

func (p *Provider) GetProject(path string) (project Project, err error) {
	if path == "" {
		return project, fmt.Errorf("Project path is required")
	}

	names := strings.Split(path, string(os.PathSeparator))

	if len(names) != 3 {
		return project, fmt.Errorf("Parameter 'GetProject.Path' is invalid: %s", path)
	}

	pool := names[0]
	name := names[2]

	payload := GetProjectParameters{
		Pool:  pool,
		Name:  name,
		Local: true,
	}

	err = p.sendRequestWithStruct("getProject", payload, &project)
	if err != nil {
		return project, err
	}

	return project, nil
}

func (p *Provider) DeleteProject(path string) error {
	if path == "" {
		return fmt.Errorf("Project path is required")
	}

	parameters := [1]string{path}

	return p.sendRequest("deleteProject", parameters)
}

type CreateProjectParameters struct {
	Pool      string   `json:"poolName"`
	Name      string   `json:"projectName"`
	Protocols []string `json:"intendedProtocolList"`
}

func (p *Provider) CreateProject(path string) error {
	if path == "" {
		return fmt.Errorf("Project path is required")
	}

	names := strings.Split(path, string(os.PathSeparator))

	if len(names) != 3 {
		return fmt.Errorf("Parameter 'CreateProject.Path' is invalid: %s", path)
	}

	pool := names[0]
	name := names[2]

	protocols := []string{"NFS", "SMB", "iSCSI"}
	parameters := []CreateProjectParameters{
		{
			Pool:      pool,
			Name:      name,
			Protocols: protocols,
		},
	}

	return p.sendRequest("createProject", parameters)
}

// GetFilesystemAvailableCapacity returns NexentaStor filesystem available size by its path
func (p *Provider) GetFilesystemAvailableCapacity(path string) (int64, error) {
	filesystem, err := p.GetFilesystem(path)
	if err != nil {
		return 0, err
	}

	return filesystem.BytesAvailable, nil
}

func (p *Provider) GetReferencedQuotaSize(path string) (int64, error) {
	if path == "" {
		return 0, fmt.Errorf("Filesystem path is empty")
	}

	data := [1]string{path}
	share := Share_v2{}
	err := p.sendRequestWithStruct("getShare", data, &share)
	if err != nil {
		return 0, err
	}
	return share.QuotaSize, nil
}

func (p *Provider) GetFilesystem(path string) (filesystem Filesystem, err error) {
	if path == "" {
		return filesystem, fmt.Errorf("Filesystem path is empty")
	}

	data := [1]string{path}
	share := Share_v2{}
	err = p.sendRequestWithStruct("getShare", data, &share)
	if err != nil {
		return filesystem, err
	}

	filesystem = p.Share_v2toFilesystem(share)

	return filesystem, nil
}

// GetVolumesWithStartingToken returns volumes by parent volumeGroup after specified starting token
// parent - parent volumeGroup's path
// startingToken - a path to a specific volume to start AFTER this token
// limit - the maximum count of volumes to return in the list
// Function may return nextToken if there is more volumes than limit value
func (p *Provider) GetVolumesWithStartingToken(parent string, startingToken string, limit int) (
	volumes []Volume,
	nextToken string,
	err error,
) {
	startingTokenFound := false
	if startingToken == "" {
		// if no startingToken set then filesystem list should starts with the first one
		startingTokenFound = true
	}

	// if no limit set then all filesystem after startingToken should be in the response
	noLimit := limit == 0

	// load volumes using slice requests
	offset := 0
	lastResultCount := nsFilesystemListLimit
	for (noLimit || len(volumes) < limit) && lastResultCount >= nsFilesystemListLimit {
		volumesSlice, err := p.GetVolumesSlice(parent, nsFilesystemListLimit-1, offset)
		if err != nil {
			return nil, "", err
		}
		for _, fs := range volumesSlice {
			if startingTokenFound {
				volumes = append(volumes, fs)
				if len(volumes) == limit {
					nextToken = fs.Path
					break
				}
			} else if fs.Path == startingToken {
				startingTokenFound = true
			}
		}
		lastResultCount = len(volumesSlice)
		offset += lastResultCount
	}

	return volumes, nextToken, nil
}

// GetVolumes returns all NexentaStor volumes by parent volumeGroup
func (p *Provider) GetVolumes(parent string) ([]Volume, error) {
	volumes := []Volume{}

	offset := 0
	lastResultCount := nsFilesystemListLimit
	for lastResultCount >= nsFilesystemListLimit {
		volumesSlice, err := p.GetVolumesSlice(parent, nsFilesystemListLimit-1, offset)
		if err != nil {
			return nil, err
		}
		for _, vol := range volumesSlice {
			volumes = append(volumes, vol)
		}
		lastResultCount = len(volumesSlice)
		offset += lastResultCount
	}

	return volumes, nil
}

// GetFilesystems returns all NexentaStor filesystems by parent filesystem
func (p *Provider) GetFilesystems(parent string) ([]Filesystem, error) {
	filesystems := []Filesystem{}

	offset := 0
	lastResultCount := nsFilesystemListLimit
	for lastResultCount >= nsFilesystemListLimit {
		filesystemsSlice, err := p.GetFilesystemsSlice(parent, nsFilesystemListLimit-1, offset)
		if err != nil {
			return nil, err
		}
		for _, fs := range filesystemsSlice {
			filesystems = append(filesystems, fs)
		}
		lastResultCount = len(filesystemsSlice)
		offset += lastResultCount
	}

	return filesystems, nil
}

// GetFilesystemsWithStartingToken returns filesystems by parent filesystem after specified starting token
// parent - parent filesystem's path
// startingToken - a path to a specific filesystem to start AFTER this token
// limit - the maximum count of filesystems to return in the list
// Function may return nextToken if there is more filesystems than limit value
func (p *Provider) GetFilesystemsWithStartingToken(parent string, startingToken string, limit int) (
	filesystems []Filesystem,
	nextToken string,
	err error,
) {
	startingTokenFound := false
	if startingToken == "" {
		// if no startingToken set then filesystem list should starts with the first one
		startingTokenFound = true
	}

	// if no limit set then all filesystem after startingToken should be in the response
	noLimit := limit == 0

	// load filesystems using slice requests
	offset := 0
	lastResultCount := nsFilesystemListLimit
	for (noLimit || len(filesystems) < limit) && lastResultCount >= nsFilesystemListLimit {
		filesystemsSlice, err := p.GetFilesystemsSlice(parent, nsFilesystemListLimit-1, offset)
		if err != nil {
			return nil, "", err
		}
		for _, fs := range filesystemsSlice {
			if startingTokenFound {
				filesystems = append(filesystems, fs)
				if len(filesystems) == limit {
					nextToken = fs.Path
					break
				}
			} else if fs.Path == startingToken {
				startingTokenFound = true
			}
		}
		lastResultCount = len(filesystemsSlice)
		offset += lastResultCount
	}

	return filesystems, nextToken, nil
}

type ListSharesParams struct {
	Pool    string
	Project string
	Local   bool
}

func (p ListSharesParams) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Pool, p.Project, p.Local}
	return json.Marshal(list)
}

// GetFilesystemsSlice returns a slice of filesystems by parent filesystem with specified limit and offset
// offset - the first record number of collection, that would be included in result
func (p *Provider) GetFilesystemsSlice(parent string, limit, offset int) (filesystems []Filesystem, err error) {
	if limit <= 0 || limit >= nsFilesystemListLimit {
		return nil, fmt.Errorf(
			"GetFilesystemsSlice(): parameter 'limit' must be greater that 0 and less than %d, got: %d",
			nsFilesystemListLimit,
			limit,
		)
	} else if offset < 0 {
		return nil, fmt.Errorf(
			"GetFilesystemsSlice(): parameter 'offset' must be greater or equal to 0, got: %d",
			offset,
		)
	}

	path := strings.Split(parent, string(os.PathSeparator))

	if len(path) != 3 {
		return nil, fmt.Errorf("Parameter 'parent' is invalid: %s", parent)
	}

	sharesParams := ListSharesParams{
		Pool:    path[0],
		Project: path[2],
		Local:   true,
	}

	shares := []Share_v1{}
	err = p.sendRequestWithStruct("listShares", sharesParams, &shares)
	if err != nil {
		return nil, err
	}

	fmt.Println("===> FULL", shares)

	for count, share := range shares {
		if count >= offset && count < (offset+limit) {
			fmt.Println("===> count", count, "offset", offset, "limit", limit, " ADD", share)
			filesystem := p.Share_v1toFilesystem(share)
			filesystems = append(filesystems, filesystem)
		}
	}

	fmt.Println("===> RET", filesystems)
	return filesystems, nil
}

// GetVolumesSlice returns a slice of volumes by parent volumeGroup with specified limit and offset
// offset - the first record number of collection, that would be included in result
func (p *Provider) GetVolumesSlice(parent string, limit, offset int) ([]Volume, error) {
	if limit <= 0 || limit >= nsFilesystemListLimit {
		return nil, fmt.Errorf(
			"GetVolumesSlice(): parameter 'limit' must be greater that 0 and less than %d, got: %d",
			nsFilesystemListLimit,
			limit,
		)
	} else if offset < 0 {
		return nil, fmt.Errorf(
			"GetVolumesSlice(): parameter 'offset' must be greater or equal to 0, got: %d",
			offset,
		)
	}

	uri := p.RestClient.BuildURI("/storage/volumes", map[string]string{
		"parent": parent,
		"limit":  fmt.Sprint(limit),
		"offset": fmt.Sprint(offset),
	})

	response := nefStorageVolumesResponse{}
	err := p.sendRequestWithStruct(uri, nil, &response)
	if err != nil {
		return nil, err
	}

	volumes := []Volume{}
	for _, fs := range response.Data {
		volumes = append(volumes, fs)
	}

	return volumes, nil
}

// CreateFilesystemParams - params to create filesystem
type CreateFilesystemParams struct {
	// filesystem path w/o leading slash
	Path string `json:"path"`
	// filesystem referenced quota size in bytes
	ReferencedQuotaSize int64 `json:"referencedQuotaSize,omitempty"`
}

type ShareOptions struct {
	BlockSize   string `json:"blockSize,omitempty"`
	Quota       int64  `json:"quota,omitempty"`
	Reservation int64  `json:"reservation,omitempty"`
}

type SharePermissions struct {
	SharePermissionEnum int `json:"sharePermissionEnum"`
	SharePermissionMode int `json:"sharePermissionMode"`
}

type CreateShareParams struct {
	Pool        string
	Project     string
	Name        string
	Options     ShareOptions
	Permissions []SharePermissions
}

func (p CreateShareParams) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Pool, p.Project, p.Name, p.Options, p.Permissions}
	return json.Marshal(list)
}

func (p *Provider) CreateFilesystem(params CreateFilesystemParams) error {
	if params.Path == "" {
		return fmt.Errorf("Parameter 'CreateFilesystemParams.Path' is required")
	}

	shareOptions := ShareOptions{}

	if params.ReferencedQuotaSize != 0 {
		shareOptions.Quota = params.ReferencedQuotaSize
	}

	sharePermissions := []SharePermissions{
		{
			SharePermissionEnum: 0,
			SharePermissionMode: 0,
		},
	}

	path := strings.Split(params.Path, string(os.PathSeparator))

	if len(path) != 4 {
		return fmt.Errorf("Parameter 'CreateFilesystemParams.Path' is invalid: %s", params.Path)
	}

	shareParams := CreateShareParams{
		Pool:        path[0],
		Project:     path[2],
		Name:        path[3],
		Options:     shareOptions,
		Permissions: sharePermissions,
	}

	return p.sendRequest("createShare", shareParams)
}

// UpdateFilesystemParams - params to update filesystem
type UpdateFilesystemParams struct {
	// filesystem referenced quota size in bytes
	ReferencedQuotaSize int64 `json:"referencedQuotaSize,omitempty"`
}

type UpdateShareParams struct {
	Path    string
	Options ShareOptions
}

func (p UpdateShareParams) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Options}
	return json.Marshal(list)
}

// UpdateFilesystem updates filesystem by path
func (p *Provider) UpdateFilesystem(path string, params UpdateFilesystemParams) error {
	if path == "" {
		return fmt.Errorf("Parameter 'path' is required")
	}

	shareOptions := ShareOptions{}
	if params.ReferencedQuotaSize == 0 {
		shareOptions.Quota = -1
	} else {
		shareOptions.Quota = params.ReferencedQuotaSize
	}

	shareParams := UpdateShareParams{
		Path:    path,
		Options: shareOptions,
	}

	return p.sendRequest("modifyShareProperties", shareParams)
}

// DestroyFilesystemParams - filesystem deletion parameters
type DestroyFilesystemParams struct {
	// If set to `true`, then tries to destroy filesystem's snapshots as well.
	// In case some snapshots have clones, the filesystem cannot be deleted
	// without deleting all dependent clones, OR promoting one of the clones
	// to take over the snapshots (see "PromoteMostRecentCloneIfExists" parameter).
	DestroySnapshots bool

	// If set to `true`, then tries to find the most recent snapshot clone and if found one,
	// that clone will be promoted to take over all the snapshots from the original filesystem,
	// then the original filesystem will be destroyed.
	//
	// Initial state:
	//    [fsSource]---+                       // source filesystem
	//                 |    [snapshot1]        // source filesystem snapshots
	//                 |    [snapshot2]
	//                 `--->[snapshot3]<---+
	//                                     |
	//    [fsClone1]-----------------------+   // filesystem clone of "snapshot3"
	//    [fsClone2]-----------------------+   // another filesystem clone of "snapshot3"
	//
	// After destroy "fsSource" filesystem call (PromoteMostRecentCloneIfExists=true and DestroySnapshots=true):
	//    [fsClone1]<----------------------+   // "fsClone1" is still linked to "snapshot3"
	//    [fsClone2]---+                   |   // "fsClone2" is got promoted to take over snapshots of "fsSource"
	//                 |    [snapshot1]    |
	//                 |    [snapshot2]    |
	//                 `--->[snapshot3]<---+
	//
	PromoteMostRecentCloneIfExists bool
}

type DeleteShareParams struct {
	Path            string
	Recursive       bool
	ErrorIfNotFound bool
	Promote         bool
}

func (p DeleteShareParams) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Recursive, p.ErrorIfNotFound, p.Promote}
	return json.Marshal(list)
}

// DestroyFilesystem destroys filesystem on NS, may destroy snapshots and promote clones (see DestroyFilesystemParams)
// Path format: 'pool/dataset/filesystem'
func (p *Provider) DestroyFilesystem(path string, params DestroyFilesystemParams) error {
	shareParams := DeleteShareParams{
		Path:            path,
		Recursive:       params.DestroySnapshots,
		ErrorIfNotFound: false,
		Promote:         params.PromoteMostRecentCloneIfExists,
	}

	return p.sendRequest("deleteShare", shareParams)
}

// CreateNfsShareParams - params to create NFS share
type CreateNfsShareParams struct {
	// filesystem path w/o leading slash
	Filesystem    string        `json:"filesystem"`
	ReadWriteList []NfsRuleList `json:"readWriteList"`
	ReadOnlyList  []NfsRuleList `json:"readOnlyList"`
}

type NfsAcl struct {
	Type       string `json:"hostType"`
	Host       string `json:"host"`
	AccessMode string `json:"accessMode"`
	RootAccess bool   `json:"rootAccessForNFS"`
}

type SetNfsParams struct {
	Path string
	Acl  []NfsAcl
}

func (p SetNfsParams) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Acl}
	return json.Marshal(list)
}

type NfsShare struct {
	Path    string
	Enabled bool
}

func (p NfsShare) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Enabled}
	return json.Marshal(list)
}

// CreateNfsShare creates NFS share on specified filesystem
// CLI test:
//   showmount -e HOST
//   mkdir -p /mnt/test && sudo mount -v -t nfs HOST:/pool/fs /mnt/test
//   findmnt /mnt/test
func (p *Provider) CreateNfsShare(params CreateNfsShareParams) error {
	if params.Filesystem == "" {
		return fmt.Errorf("CreateNfsShareParams.Filesystem is required")
	}

	nfsshare := NfsShare{
		Path:    params.Filesystem,
		Enabled: true,
	}

	p.sendRequest("setNFSSharingOnShare", nfsshare)

	nfsacls := []NfsAcl{}
	for _, rw := range params.ReadWriteList {
		nfsacl := NfsAcl{
			AccessMode: "rw",
			RootAccess: true,
		}
		if rw.Etype == "fqdn" || rw.Etype == "domain" {
			nfsacl.Type = "FQDN"
			nfsacl.Host = rw.Entity
		} else {
			nfsacl.Type = "IP"
			if rw.Mask > 0 {
				nfsacl.Host = fmt.Sprintf("%s/%d", rw.Entity, rw.Mask)
			} else {
				nfsacl.Host = rw.Entity
			}
		}
		nfsacls = append(nfsacls, nfsacl)
	}

	for _, ro := range params.ReadOnlyList {
		nfsacl := NfsAcl{
			AccessMode: "ro",
			RootAccess: true,
		}
		if ro.Etype == "fqdn" || ro.Etype == "domain" {
			nfsacl.Type = "FQDN"
			nfsacl.Host = ro.Entity
		} else {
			nfsacl.Type = "IP"
			if ro.Mask > 0 {
				nfsacl.Host = fmt.Sprintf("%s/%d", ro.Entity, ro.Mask)
			} else {
				nfsacl.Host = ro.Entity
			}
		}
		nfsacls = append(nfsacls, nfsacl)
	}

	nfsparams := SetNfsParams{
		Path: params.Filesystem,
		Acl:  nfsacls,
	}

	return p.sendRequest("setNFSNetworkACLsOnShare", nfsparams)
}

// DeleteNfsShare destroys NFS chare by filesystem path
func (p *Provider) DeleteNfsShare(path string) error {
	if path == "" {
		return fmt.Errorf("Filesystem path is empty")
	}

	params := [1]string{path}

	return p.sendRequest("removeAllNFSNetworkACLsOnShare", params)
}

// CreateSmbShareParams - params to create SMB share
type CreateSmbShareParams struct {
	// filesystem path w/o leading slash
	Filesystem string `json:"filesystem"`
	// share name, used in mount command
	ShareName     string        `json:"shareName,omitempty"`
	ReadWriteList []NfsRuleList `json:"readWriteList"`
	ReadOnlyList  []NfsRuleList `json:"readOnlyList"`
}

type SmbAcl struct {
	Type       string `json:"hostType"`
	Host       string `json:"host"`
	AccessMode string `json:"accessMode"`
}

type SetSmbParams struct {
	Path string
	Acl  []SmbAcl
}

func (p SetSmbParams) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Acl}
	return json.Marshal(list)
}

type SmbShare struct {
	Path    string
	Enabled bool
	Name    string
	Guest   bool
}

func (p SmbShare) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Enabled, p.Name, p.Guest}
	return json.Marshal(list)
}

// CreateSmbShare creates SMB share (cifs) on specified filesystem
// Leave shareName empty to generate default value
// CLI test:
//   mkdir -p /mnt/test && sudo mount -v -t cifs -o username=admin,password=Nexenta@1 //HOST//pool_fs /mnt/test
//   findmnt /mnt/test
func (p *Provider) CreateSmbShare(params CreateSmbShareParams) error {
	if params.Filesystem == "" {
		return fmt.Errorf("CreateSmbShareParams.Filesystem is required")
	}

	filesystem := Filesystem{
		Path: params.Filesystem,
	}

	sharename := params.ShareName
	if sharename == "" {
		sharename = filesystem.GetDefaultSmbShareName()
	}

	smbshare := SmbShare{
		Path:    filesystem.Path,
		Enabled: true,
		Name:    sharename,
		Guest:   false,
	}

	p.sendRequest("setSMBSharingOnShare", smbshare)

	smbacls := []SmbAcl{}
	for _, rw := range params.ReadWriteList {
		smbacl := SmbAcl{
			AccessMode: "rw",
		}
		if rw.Etype == "fqdn" || rw.Etype == "domain" {
			smbacl.Type = "FQDN"
			smbacl.Host = rw.Entity
		} else {
			smbacl.Type = "IP"
			if rw.Mask > 0 {
				smbacl.Host = fmt.Sprintf("%s/%d", rw.Entity, rw.Mask)
			} else {
				smbacl.Host = rw.Entity
			}
		}
		smbacls = append(smbacls, smbacl)
	}

	for _, ro := range params.ReadOnlyList {
		smbacl := SmbAcl{
			AccessMode: "ro",
		}
		if ro.Etype == "fqdn" || ro.Etype == "domain" {
			smbacl.Type = "FQDN"
			smbacl.Host = ro.Entity
		} else {
			smbacl.Type = "IP"
			if ro.Mask > 0 {
				smbacl.Host = fmt.Sprintf("%s/%d", ro.Entity, ro.Mask)
			} else {
				smbacl.Host = ro.Entity
			}
		}
		smbacls = append(smbacls, smbacl)
	}

	smbparams := SetSmbParams{
		Path: params.Filesystem,
		Acl:  smbacls,
	}

	return p.sendRequest("setSMBNetworkACLsOnShare", smbparams)
}

// GetSmbShareName returns share name for filesystem that shared over SMB
func (p *Provider) GetSmbShareName(path string) (sharename string, err error) {
	if path == "" {
		return "", fmt.Errorf("Filesystem path is required")
	}

	data := [1]string{path}
	share := Share_v2{}
	err = p.sendRequestWithStruct("getShare", data, &share)
	if err != nil {
		return sharename, err
	}

	return share.ShareName, nil
}

// DeleteSmbShare destroys SMB share by filesystem path
func (p *Provider) DeleteSmbShare(path string) error {
	if path == "" {
		return fmt.Errorf("Filesystem path is empty")
	}

	params := [1]string{path}

	return p.sendRequest("removeAllSMBNetworkACLsOnShare", params)

}

// SetFilesystemACL sets filesystem ACL, so NFS share can allow user to write w/o checking UNIX user uid
func (p *Provider) SetFilesystemACL(path string, aclRuleSet ACLRuleSet) error {
	if path == "" {
		return fmt.Errorf("Filesystem path is required")
	}

	uri := fmt.Sprintf("/storage/filesystems/%s/acl", url.PathEscape(path))

	permissions := []string{}
	if aclRuleSet == ACLReadOnly {
		permissions = append(permissions, "read_set")
	} else {
		permissions = append(permissions, "full_set")
	}

	data := &nefStorageFilesystemsACLRequest{
		Type:      "allow",
		Principal: "everyone@",
		Flags: []string{
			"file_inherit",
			"dir_inherit",
		},
		Permissions: permissions,
	}

	return p.sendRequest(uri, data)
}

// CreateSnapshotParams - params to create snapshot
type CreateSnapshotParams struct {
	// snapshot path w/o leading slash
	Path string `json:"path"`
}

type CreateSnapshotOptions struct {
	Share   Share_v1
	Name    string
	Quiesce bool
}

func (p CreateSnapshotOptions) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Share, p.Name, p.Quiesce}
	return json.Marshal(list)
}

// CreateSnapshot creates snapshot by filesystem path
func (p *Provider) CreateSnapshot(params CreateSnapshotParams) error {
	if params.Path == "" {
		return fmt.Errorf("Parameter 'CreateSnapshotParams.Path' is required")
	}

	elements := strings.Split(params.Path, "@")

	if len(elements) != 2 {
		return fmt.Errorf("Parameter 'CreateSnapshotParams.Path' is invalid")
	}

	path := elements[0]
	snapshotName := elements[1]

	names := strings.Split(path, string(os.PathSeparator))

	if len(names) != 4 {
		return fmt.Errorf("Parameter 'CreateSnapshotParams.Path' is invalid")
	}

	poolName := names[0]
	projectName := names[2]
	shareName := names[3]

	share := Share_v1{
		PoolName:    poolName,
		ProjectName: projectName,
		Name:        shareName,
		Local:       true,
	}

	snapshot := CreateSnapshotOptions{
		Share:   share,
		Name:    snapshotName,
		Quiesce: false,
	}

	return p.sendRequest("createShareSnapshot", snapshot)
}

type listSnapshotsParameters struct {
	Path    string
	Pattern string
}

func (p listSnapshotsParameters) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Pattern}
	return json.Marshal(list)
}

// GetSnapshot returns snapshot by its path
// path - full path to snapshot w/o leading slash (e.g. "p/d/fs@s")
func (p *Provider) GetSnapshot(path string) (snapshot Snapshot, err error) {
	if path == "" {
		return snapshot, fmt.Errorf("Snapshot path is empty")
	}

	elements := strings.Split(path, "@")

	if len(elements) != 2 {
		return snapshot, fmt.Errorf("Parameter 'GetSnapshot.Path' is invalid")
	}

	parent := elements[0]
	name := elements[1]

	payload := listSnapshotsParameters{
		Path:    parent,
		Pattern: fmt.Sprintf("%s%s", ZebiSnapshotPrefix, name),
	}

	results := []string{}
	err = p.sendRequestWithStruct("listSnapshots", payload, &results)

	if err != nil {
		return snapshot, err
	}

	if len(results) != 1 {
		return snapshot, fmt.Errorf("Snapshot %s not found %s", path, results)
	}

	snapshot = Snapshot{
		Path:   path,
		Name:   name,
		Parent: parent,
	}

	return snapshot, nil
}

// GetSnapshots returns snapshots by volume path
func (p *Provider) GetSnapshots(parent string, recursive bool) (snapshots []Snapshot, err error) {
	if parent == "" {
		return snapshots, fmt.Errorf("Parent path is empty")
	}

	payload := listSnapshotsParameters{
		Path:    parent,
		Pattern: ".*",
	}

	results := []string{}
	err = p.sendRequestWithStruct("listSnapshots", payload, &results)

	if err != nil {
		return snapshots, err
	}

	for _, item := range results {
		name := strings.TrimPrefix(item, ZebiSnapshotPrefix)
		snapshot := Snapshot{
			Path:   fmt.Sprintf("%s@%s", parent, name),
			Name:   name,
			Parent: parent,
		}

		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

type DeleteShareSnapshotParameters struct {
	Path      string
	Recursive bool
}

func (p DeleteShareSnapshotParameters) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Recursive}
	return json.Marshal(list)
}

// DestroySnapshot destroys snapshot by path
func (p *Provider) DestroySnapshot(path string) error {
	if path == "" {
		return fmt.Errorf("Snapshot path is required")
	}

	elements := strings.Split(path, "@")

	if len(elements) != 2 {
		return fmt.Errorf("Parameter 'DestroySnapshot.Path' is invalid")
	}

	parent := elements[0]
	name := elements[1]

	parameters := DeleteShareSnapshotParameters{
		Path:      fmt.Sprintf("%s@%s%s", parent, ZebiSnapshotPrefix, name),
		Recursive: false,
	}

	return p.sendRequest("deleteShareSnapshot", parameters)
}

// CloneSnapshotParams - params to clone snapshot to filesystem
type CloneSnapshotParams struct {
	// filesystem path w/o leading slash
	TargetPath          string `json:"targetPath"`
	ReferencedQuotaSize int64  `json:"referencedQuotaSize,omitempty"`
}

type CloneShareSnapshotParameters struct {
	Path    string
	Name    string
	Inherit bool
}

func (p CloneShareSnapshotParameters) MarshalJSON() ([]byte, error) {
	list := []interface{}{p.Path, p.Name, p.Inherit}
	return json.Marshal(list)
}

// CloneSnapshot clones snapshot to FS
func (p *Provider) CloneSnapshot(path string, params CloneSnapshotParams) error {
	if path == "" {
		return fmt.Errorf("Snapshot path is required")
	}

	if params.TargetPath == "" {
		return fmt.Errorf("Parameter 'CloneSnapshotParams.TargetPath' is required")
	}

	elements := strings.Split(path, "@")

	if len(elements) != 2 {
		return fmt.Errorf("Parameter 'CloneSnapshot.Path' is invalid")
	}

	parent := elements[0]
	name := elements[1]

	elements = strings.Split(params.TargetPath, string(os.PathSeparator))
	targetName := elements[3]

	parameters := CloneShareSnapshotParameters{
		Path:    fmt.Sprintf("%s@%s%s", parent, ZebiSnapshotPrefix, name),
		Name:    targetName,
		Inherit: false,
	}

	err := p.sendRequest("cloneShareSnapshot", parameters)
	if err != nil {
		return err
	}

	if params.ReferencedQuotaSize == 0 {
		return nil
	}

	shareOptions := ShareOptions{
		Quota: params.ReferencedQuotaSize,
	}

	shareParams := UpdateShareParams{
		Path:    fmt.Sprintf("%s/%s/%s/%s", elements[0], elements[1], elements[2], targetName),
		Options: shareOptions,
	}

	return p.sendRequest("modifyShareProperties", shareParams)

}

// GetVolume - returns NexentaStor volume properties
func (p *Provider) GetVolume(path string) (volume Volume, err error) {
	if path == "" {
		return volume, fmt.Errorf("Volume path is empty")
	}

	uri := p.RestClient.BuildURI("/storage/volumes", map[string]string{
		"path": path,
	})

	response := nefStorageVolumesResponse{}
	err = p.sendRequestWithStruct(uri, nil, &response)
	if err != nil {
		return response.Data[0], err
	}

	if len(response.Data) == 0 {
		return volume, &NefError{Code: "ENOENT", Err: fmt.Errorf("VolumeGroup '%s' not found", path)}
	}

	return response.Data[0], nil
}

// GetVolumeGroup returns NexentaStor volumeGroup by its path
func (p *Provider) GetVolumeGroup(path string) (volumeGroup VolumeGroup, err error) {
	if path == "" {
		return volumeGroup, fmt.Errorf("VolumeGroup path is empty")
	}

	uri := p.RestClient.BuildURI("/storage/volumeGroups", map[string]string{
		"path": path,
	})

	response := nefStorageVolumeGroupsResponse{}
	err = p.sendRequestWithStruct(uri, nil, &response)
	if err != nil {
		return volumeGroup, err
	}

	if len(response.Data) == 0 {
		return volumeGroup, &NefError{Code: "ENOENT", Err: fmt.Errorf("VolumeGroup '%s' not found", path)}
	}

	return response.Data[0], nil
}

// CreateVolumeParams - params to create a volume
type CreateVolumeParams struct {
	// volume path w/o leading slash
	Path       string `json:"path"`
	VolumeSize int64  `json:"volumeSize"`
}

// CreateVolume creates volume by path and size
func (p *Provider) CreateVolume(params CreateVolumeParams) error {
	if params.Path == "" {
		return fmt.Errorf(
			"Parameters 'Volume.Path' is required, received %+v", params)
	}

	return p.sendRequest("/storage/volumes", params)
}

// UpdateVolumeParams - params to update volume
type UpdateVolumeParams struct {
	// volume referenced quota size in bytes
	VolumeSize int64 `json:"volumeSize,omitempty"`
}

// UpdateVolume updates volume by path
func (p *Provider) UpdateVolume(path string, params UpdateVolumeParams) error {
	if path == "" {
		return fmt.Errorf("Parameter 'path' is required")
	}

	uri := fmt.Sprintf("/storage/volumes/%s", url.PathEscape(path))
	return p.sendRequest(uri, params)
}

// GetLunMapping returns NexentaStor lunmapping for a volume
func (p *Provider) GetLunMapping(path string) (lunMapping LunMapping, err error) {
	if path == "" {
		return lunMapping, fmt.Errorf("Volume path is empty")
	}
	uri := p.RestClient.BuildURI("/san/lunMappings", map[string]string{
		"volume": path,
		"fields": "id,volume,targetGroup,hostGroup,lun",
	})
	response := nefLunMappingsResponse{}
	err = p.sendRequestWithStruct(uri, nil, &response)
	if err != nil {
		return lunMapping, err
	}
	if len(response.Data) == 0 {
		return lunMapping, &NefError{Code: "ENOENT", Err: fmt.Errorf("lunMapping '%s' not found", path)}
	}

	return response.Data[0], nil
}

// CreateISCSITargetParamas - params to create new iSCSI target
type CreateISCSITargetParams struct {
	Name    string   `json:"name"`
	Portals []Portal `json:"portals"`
}

// CreateISCSITarget - create new iSCSI target on NexentaStor
func (p *Provider) CreateISCSITarget(params CreateISCSITargetParams) error {
	if params.Name == "" {
		return fmt.Errorf("Parameters 'Name' and 'Portal' are required, received: %+v", params)
	}
	err := p.sendRequest("/san/iscsi/targets", params)
	if !IsAlreadyExistNefError(err) {
		return err
	}
	return nil
}

// CreateTargetGroupParams - params to create target group
type CreateTargetGroupParams struct {
	Name    string   `json:"name"`
	Members []string `json:"members"`
}

// UpdateTargetGroupParams - params to update existing target group
type UpdateTargetGroupParams struct {
	Members []string `json:"members"`
}

// CreateUpdateTargetGroup - create new target group on NexentaStor
func (p *Provider) CreateUpdateTargetGroup(params CreateTargetGroupParams) error {
	if params.Name == "" || len(params.Members) == 0 {
		return fmt.Errorf(
			"Parameters 'Name' and 'Members' are required, received: %+v", params)
	}
	err := p.sendRequest("/san/targetgroups", params)
	if err != nil {
		if !IsAlreadyExistNefError(err) {
			return err
		} else {
			uri := fmt.Sprintf("/san/targetgroups/%s", url.PathEscape(params.Name))
			err = p.sendRequest(uri, UpdateTargetGroupParams{
				Members: params.Members,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateLunMappingParams - params to create new lun
type CreateLunMappingParams struct {
	HostGroup   string `json:"hostGroup"`
	Volume      string `json:"volume"`
	TargetGroup string `json:"targetGroup"`
}

// CreateLunMapping - creates lun for given volume
func (p *Provider) CreateLunMapping(params CreateLunMappingParams) error {
	if params.HostGroup == "" || params.Volume == "" || params.TargetGroup == "" {
		return fmt.Errorf(
			"Parameters 'HostGroup', 'Target' and 'TargetGroup' are required, received: %+v", params)
	}
	err := p.sendRequest("/san/lunMappings", params)
	if !IsAlreadyExistNefError(err) {
		return err
	}
	return nil
}

type DestroyVolumeParams struct {
	DestroySnapshots               bool
	PromoteMostRecentCloneIfExists bool
}

func (p *Provider) DestroyLunMapping(id string) error {
	if id == "" {
		return fmt.Errorf("LunMapping id is required")
	}

	uri := fmt.Sprintf("/san/lunMappings/%s", id)

	return p.sendRequest(uri, nil)
}

func (p *Provider) DestroyVolume(path string, params DestroyVolumeParams) error {
	err := p.destroyVolume(path, params.DestroySnapshots)
	if err != nil {
		return err
	}
	return nil
}

func (p *Provider) destroyVolume(path string, destroySnapshots bool) error {
	if path == "" {
		return fmt.Errorf("Filesystem path is required")
	}

	uri := p.RestClient.BuildURI(
		fmt.Sprintf("/storage/volumes/%s", url.PathEscape(path)),
		map[string]string{
			"snapshots": strconv.FormatBool(destroySnapshots),
		},
	)

	return p.sendRequest(uri, nil)
}

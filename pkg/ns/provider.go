package ns

import (
	"encoding/json"
	"fmt"
	"net/http"
//	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/Nexenta/go-nexentastor/pkg/rest"
)

const (
	checkJobStatusInterval = 3 * time.Second
	checkJobStatusTimeout  = 60 * time.Second
)

// ProviderInterface - NexentaStor provider interface
type ProviderInterface interface {
    // pools
    GetPools() ([]Pool, error)

	// filesystems
	CreateFilesystem(params CreateFilesystemParams) error
	UpdateFilesystem(path string, params UpdateFilesystemParams) error
	DestroyFilesystem(path string, params DestroyFilesystemParams) error
	SetFilesystemACL(path string, aclRuleSet ACLRuleSet) error
	GetFilesystem(path string) (Filesystem, error)
	GetFilesystemAvailableCapacity(path string) (int64, error)
	GetFilesystems(parent string) ([]Filesystem, error)
	GetFilesystemsWithStartingToken(parent string, startingToken string, limit int) ([]Filesystem, string, error)
	GetFilesystemsSlice(parent string, limit, offset int) ([]Filesystem, error)

	// filesystems - nfs share
	CreateNfsShare(params CreateNfsShareParams) error
	DeleteNfsShare(path string) error

	// filesystems - smb share
	CreateSmbShare(params CreateSmbShareParams) error
	DeleteSmbShare(path string) error
	GetSmbShareName(path string) (string, error)

	// snapshots
	CreateSnapshot(params CreateSnapshotParams) error
	DestroySnapshot(path string) error
	GetSnapshot(path string) (Snapshot, error)
	GetSnapshots(volumePath string, recursive bool) ([]Snapshot, error)
	CloneSnapshot(path string, params CloneSnapshotParams) error

	// volumes
	CreateVolume(params CreateVolumeParams) error
	GetVolume(path string) (Volume, error)
	GetVolumes(parent string) ([]Volume, error)
	UpdateVolume(path string, params UpdateVolumeParams) error
	DestroyVolume(path string, params DestroyVolumeParams) error
	GetVolumeGroup(path string) (VolumeGroup, error)
	GetVolumesWithStartingToken(parent string, startingToken string, limit int) ([]Volume, string, error)

	// iSCSI
	CreateLunMapping(params CreateLunMappingParams) error
	GetLunMapping(path string) (LunMapping, error)
	DestroyLunMapping(id string) error
	CreateISCSITarget(params CreateISCSITargetParams) error
	CreateUpdateTargetGroup(params CreateTargetGroupParams) error
}

// Provider - NexentaStor API provider
type Provider struct {
	Address    string
	Username   string
	Password   string
	RestClient rest.ClientInterface
	Log        *logrus.Entry
}

func (p *Provider) String() string {
	return p.Address
}

func (p *Provider) parseNefError(bodyBytes []byte, prefix string) error {
	var restErrorMessage string
	var restErrorCode string

	response := struct {
		Code         string `json:"code"`
		Details      string `json:"details"`
		Message      string `json:"message"`
	}{}

	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return nil
	}

	if response.Code != "" {
		restErrorCode = response.Code
	}

	if response.Message != "" {
		restErrorMessage = response.Message
	}

	if restErrorMessage != "" {
		return &NefError{
			Err:  fmt.Errorf("%s: %s", prefix, restErrorMessage),
			Code: restErrorCode,
		}
	}

	return nil
}

func (p *Provider) sendRequestWithStruct(path string, data, response interface{}) error {
	bodyBytes, err := p.doAuthRequest(path, data)
	if err != nil {
		return err
	}

	if len(bodyBytes) == 0 {
		return fmt.Errorf("Request '%s' responded with empty body", path)
	} else if !json.Valid(bodyBytes) {
		return fmt.Errorf("Request '%s' responded with invalid JSON: '%s'", path, bodyBytes)
	}

	if response != nil {
		err := json.Unmarshal(bodyBytes, response)
		if err != nil {
			return fmt.Errorf(
				"Request '%s': cannot unmarshal JSON from: '%s' to '%+v': %s",
				path,
				bodyBytes,
				response,
				err,
			)
		}
	}

	return nil
}

func (p *Provider) sendRequest(path string, data interface{}) error {
	_, err := p.doAuthRequest(path, data)
	return err
}

func (p *Provider) doAuthRequest(path string, data interface{}) ([]byte, error) {
	l := p.Log.WithField("func", "doAuthRequest()")

	statusCode, bodyBytes, err := p.RestClient.Send(path, data)
	if err != nil {
		return bodyBytes, err
	}

	nefError := p.parseNefError(bodyBytes, "checking login status")

	// log in again if user is not logged in
	if statusCode == http.StatusUnauthorized && IsAuthNefError(nefError) {
		// do login call if used is not authorized in api
		l.Debugf("log in as '%s'...", p.Username)

		// send original request again
		statusCode, bodyBytes, err = p.RestClient.Send(path, data)
		if err != nil {
			return bodyBytes, err
		}
	}

	if statusCode == http.StatusAccepted {
		// this is an async job
		//var href string
		//href, err = p.parseAsyncJobHref(bodyBytes)
		//if err != nil {
		//	return bodyBytes, err
		//}
	} else if statusCode >= 300 {
		nefError := p.parseNefError(bodyBytes, "request error")
		if nefError != nil {
			err = nefError
		} else {
			err = fmt.Errorf(
				"Request returned %d code, but response body doesn't contain explanation: %v",
				statusCode,
				bodyBytes,
			)
		}
	}

	return bodyBytes, err
}

func (p *Provider) parseAsyncJobHref(bodyBytes []byte) (string, error) {
	response := nefJobStatusResponse{}
	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		return "", fmt.Errorf("Cannot parse NS response '%s' to '%+v': %s", bodyBytes, response, err)
	}

	for _, link := range response.Links {
		if link.Rel == "monitor" && link.Href != "" {
			return link.Href, nil
		}
	}

	return "", fmt.Errorf("Request return an async job, but response doesn't contain any links: %v", bodyBytes)
}

// ProviderArgs - params to create Provider instance
type ProviderArgs struct {
	Address  string
	Username string
	Password string
	Log      *logrus.Entry

	// InsecureSkipVerify controls whether a client verifies the server's certificate chain and host name.
	InsecureSkipVerify bool
}

// NewProvider creates NexentaStor provider instance
func NewProvider(args ProviderArgs) (ProviderInterface, error) {
	l := args.Log.WithFields(logrus.Fields{
		"cmp": "NSProvider",
		"ns":  args.Address,
	})

	if args.Address == "" {
		return nil, fmt.Errorf("NexentaStor address not specified: %s", args.Address)
	}

	restClient := rest.NewClient(rest.ClientArgs{
		Address:            args.Address,
        Username:   args.Username,
        Password:   args.Password,
		Log:                l,
		InsecureSkipVerify: args.InsecureSkipVerify,
	})

	l.Debugf("created for '%s'", args.Address)
	return &Provider{
		Address:    args.Address,
		Username:   args.Username,
		Password:   args.Password,
		RestClient: restClient,
		Log:        l,
	}, nil
}

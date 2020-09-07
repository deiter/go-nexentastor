package provider_test

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/Nexenta/go-nexentastor/pkg/ns"
)

// defaults
const (
	defaultUsername     = "admin"
	defaultPassword     = "t"
	defaultPoolName     = "pool-a"
	defaultProjectName  = "csi"
	defaultFolderName   = "folder"
	defaultSnapshotName = "snapshot"
)

// count of concurrent REST calls to create filesystems on NS
const concurrentProcesses = 100

type config struct {
	address      string
	username     string
	password     string
	poolName     string
	projectName  string
	projectPath  string
	folderName   string
	folderPath   string
	smbShareName string
	snapshotName string
	snapshotPath string
	cluster      bool
}

var c *config
var l *logrus.Entry

func TestMain(m *testing.M) {
	var (
		address      = flag.String("address", "", "NS API [schema://host:port,...]")
		username     = flag.String("username", defaultUsername, "overwrite NS API username from config")
		password     = flag.String("password", defaultPassword, "overwrite NS API password from config")
		poolName     = flag.String("pool_name", defaultPoolName, "pool on NS")
		projectName  = flag.String("projectname", defaultProjectName, "Project name")
		folderName   = flag.String("foldername", defaultFolderName, "folder name")
		snapshotName = flag.String("snapshotname", defaultSnapshotName, "snapshot name")
		cluster      = flag.Bool("cluster", false, "this is a NS cluster")
		log          = flag.Bool("log", false, "show logs")
	)

	flag.Parse()

	l = logrus.New().WithField("ns", *address)
	l.Logger.SetLevel(logrus.PanicLevel)
	if *log {
		l.Logger.SetLevel(logrus.DebugLevel)
	}

	if *address == "" {
		l.Fatal("--address=[schema://host:port,...] flag cannot be empty")
	}

	c = &config{
		address:      *address,
		username:     *username,
		password:     *password,
		poolName:     *poolName,
		projectName:  *projectName,
		projectPath:  fmt.Sprintf("%s/Local/%s", *poolName, *projectName),
		folderName:   *folderName,
		folderPath:   fmt.Sprintf("%s/Local/%s/%s", *poolName, *projectName, *folderName),
		cluster:      *cluster,
		smbShareName: "testShareName",
		snapshotName: *snapshotName,
		snapshotPath: fmt.Sprintf("%s/Local/%s/%s@%s", *poolName, *projectName, *folderName, *snapshotName),
	}

	os.Exit(m.Run())
}

func TestProvider_NewProvider(t *testing.T) {
	t.Logf("Using config:\n---\n%+v\n---", c)

	testSnapshotPath := fmt.Sprintf("%s@%s", c.folderPath, c.snapshotName)
	testSnapshotCloneTargetPath := fmt.Sprintf("%s/csiDriverFsCloned", c.projectPath)

	nsp, err := ns.NewProvider(ns.ProviderArgs{
		Address:            c.address,
		Username:           c.username,
		Password:           c.password,
		Log:                l,
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Error(err)
	}

	t.Run("GetPools()", func(t *testing.T) {
		pools, err := nsp.GetPools()
		if err != nil {
			t.Error(err)
		} else if !poolArrayContains(pools, c.poolName) {
			t.Errorf("Pool %s doesn't exist on NS %s", c.poolName, c.address)
		}
	})

	t.Run("GetFilesystem() not exists", func(t *testing.T) {
		nonExistingName := fmt.Sprintf("%s-%s", c.folderPath, "non-existing")
		filesystem, err := nsp.GetFilesystem(nonExistingName)
		if err != nil && !ns.ErrorZebiResourceNotFound(err) {
			t.Error(err)
		} else if filesystem.Path != "" {
			t.Errorf("Filesystem %s should not exist, but found in the result: %v", nonExistingName, filesystem)
		}
	})

	t.Run("CreateFilesystem()", func(t *testing.T) {
		err := cleanupProject(nsp, c.projectPath)

		if err != nil {
			t.Error(err)
			return
		}

		err = nsp.CreateFilesystem(ns.CreateFilesystemParams{
			Path: c.folderPath,
		})

		if err != nil {
			t.Error(err)
			return
		}

		filesystems, err := nsp.GetFilesystems(c.projectPath)
		fmt.Println(" =====> fs list", filesystems)

		if err != nil {
			t.Error(err)
		} else if !filesystemArrayContains(filesystems, c.folderPath) {
			t.Errorf("New filesystem %s wasn't created on NS %s", c.folderPath, c.address)
		}
	})

	t.Run("GetFilesystems()", func(t *testing.T) {
		filesystems, err := nsp.GetFilesystems(c.projectPath)
		if err != nil {
			t.Error(err)
		} else if filesystemArrayContains(filesystems, c.poolName) {
			t.Errorf("Pool %s should not be in the results", c.poolName)
		} else if !filesystemArrayContains(filesystems, c.folderPath) {
			t.Errorf("Dataset %s doesn't exist", c.folderPath)
		}
	})

	t.Run("GetFilesystem() exists", func(t *testing.T) {
		filesystem, err := nsp.GetFilesystem(c.folderPath)
		if err != nil {
			t.Error(err)
		} else if filesystem.Path != c.folderPath {
			t.Errorf("No %s filesystem in the result", c.folderPath)
		}
	})

	t.Run("GetFilesystem() created filesystem should not be shared", func(t *testing.T) {
		filesystem, err := nsp.GetFilesystem(c.folderPath)
		if err != nil {
			t.Error(err)
		} else if filesystem.SharedOverNfs {
			t.Errorf("Created filesystem %s should not be shared over NFS (NS %s)", c.folderPath, c.address)
		} else if filesystem.SharedOverSmb {
			t.Errorf("Created filesystem %s should not be shared over SMB (NS %s)", c.folderPath, c.address)
		}
	})

	t.Run("CreateNfsShare()", func(t *testing.T) {
		//nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: c.folderPath})

		err = nsp.CreateNfsShare(ns.CreateNfsShareParams{
			Filesystem: c.folderPath,
		})
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("GetFilesystem() created filesystem should be shared over NFS", func(t *testing.T) {
		filesystem, err := nsp.GetFilesystem(c.folderPath)
		if err != nil {
			t.Error(err)
		} else if !filesystem.SharedOverNfs {
			t.Errorf("Created filesystem %s should be shared (NS %s)", c.folderPath, c.address)
		}
	})

	t.Run("nfs share should appear on NS", func(t *testing.T) {
		URL, err := url.Parse(c.address)
		if err != nil {
			t.Error(err)
		}

		host := URL.Hostname()

		filesystem, err := nsp.GetFilesystem(c.folderPath)
		if err != nil {
			t.Error(err)
		}

		out, err := exec.Command("showmount", "-e", host).Output()
		if err != nil {
			t.Error(err)
		} else if !strings.Contains(fmt.Sprintf("%s", out), filesystem.MountPoint) {
			t.Errorf("Cannot find '%s' nfs in the 'showmount' output: \n---\n%s\n---\n", c.folderPath, out)
		}
	})

	t.Run("DeleteNfsShare()", func(t *testing.T) {
		filesystems, err := nsp.GetFilesystems(c.projectPath)
		if err != nil {
			t.Error(err)
			return
		} else if !filesystemArrayContains(filesystems, c.folderPath) {
			t.Skipf("Filesystem %s doesn't exist on NS %s", c.folderPath, c.address)
			return
		}

		err = nsp.DeleteNfsShare(c.folderPath)
		if err != nil {
			t.Error(err)
		}
	})

	for _, smbShareName := range []string{c.smbShareName, ""} {
		smbShareName := smbShareName

		t.Run(
			fmt.Sprintf("CreateSmbShare() should create SMB share with '%s' share name", smbShareName),
			func(t *testing.T) {
				nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: c.folderPath})

				err = nsp.CreateSmbShare(ns.CreateSmbShareParams{
					Filesystem: c.folderPath,
					ShareName:  smbShareName,
				})
				if err != nil {
					t.Error(err)
				}
			},
		)

		t.Run("GetFilesystem() created filesystem should be shared over SMB", func(t *testing.T) {
			filesystem, err := nsp.GetFilesystem(c.folderPath)
			if err != nil {
				t.Error(err)
			} else if !filesystem.SharedOverSmb {
				t.Errorf("Created filesystem %s should be shared over SMB (NS %s)", c.folderPath, c.address)
			}
		})

		t.Run("GetSmbShareName() should return SMB share name", func(t *testing.T) {
			filesystem, err := nsp.GetFilesystem(c.folderPath)
			if err != nil {
				t.Error(err)
				return
			}

			var expectedShareName string
			if smbShareName == "" {
				expectedShareName = filesystem.GetDefaultSmbShareName()
			} else {
				expectedShareName = smbShareName
			}

			shareName, err := nsp.GetSmbShareName(c.folderPath)
			if err != nil {
				t.Error(err)
			} else if shareName != expectedShareName {
				t.Errorf(
					"Expected shareName='%s' but got '%s', for filesystem '%s' on NS %s",
					expectedShareName,
					shareName,
					c.folderPath,
					c.address,
				)
			}
		})

		//TODO test SMB share, mount cifs?

		t.Run("DeleteSmbShare()", func(t *testing.T) {
			err = nsp.DeleteSmbShare(c.folderPath)
			if err != nil {
				t.Error(err)
			}
		})
	}

	t.Run("DestroyFilesystem()", func(t *testing.T) {
		nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})
		nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: c.folderPath})

		err = nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{DestroySnapshots: true})
		if err != nil {
			t.Error(err)
			return
		}

		filesystems, err := nsp.GetFilesystems(c.projectPath)
		if err != nil {
			t.Error(err)
		} else if filesystemArrayContains(filesystems, c.folderPath) {
			t.Errorf("Filesystem %s still exists on NS %s", c.folderPath, c.address)
		}
	})

	t.Run("CreateFilesystem() with referenced quota size", func(t *testing.T) {
		nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})

		var referencedQuotaSize int64 = 2 * 1024 * 1024 * 1024

		err = nsp.CreateFilesystem(ns.CreateFilesystemParams{
			Path:                c.folderPath,
			ReferencedQuotaSize: referencedQuotaSize,
		})
		if err != nil {
			t.Error(err)
			return
		}

		filesystem, err := nsp.GetFilesystem(c.folderPath)
		if err != nil {
			t.Error(err)
			return
		} else if filesystem.QuotaSize != referencedQuotaSize {
			t.Errorf(
				"New filesystem %s referenced quota size expected to be %d, but got %d (NS %s)",
				filesystem.Path,
				referencedQuotaSize,
				filesystem.QuotaSize,
				c.address,
			)
		}
	})

	t.Run("CreateSnapshot()", func(t *testing.T) {
		nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})

		nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: c.folderPath})

		err = nsp.CreateSnapshot(ns.CreateSnapshotParams{
			Path: testSnapshotPath,
		})

		if err != nil {
			t.Error(err)
		}

		snapshot, err := nsp.GetSnapshot(testSnapshotPath)

		if err != nil {
			t.Error(err)
			return
		} else if snapshot.Path != testSnapshotPath {
			t.Errorf(
				"Created snapshot path expected to be '%s', but got '%s' (Snapshot: %+v, NS %s)",
				testSnapshotPath,
				snapshot.Path,
				snapshot,
				c.address,
			)
			return
		} else if snapshot.Name != c.snapshotName {
			t.Errorf(
				"Created snapshot name expected to be '%s', but got '%s' (Snapshot: %+v, NS %s)",
				c.snapshotName,
				snapshot.Name,
				snapshot,
				c.address,
			)
			return
		} else if snapshot.Parent != c.folderPath {
			t.Errorf(
				"Created snapshot parent expected to be '%s', but got '%s' (Snapshot: %+v, NS %s)",
				c.folderPath,
				snapshot.Parent,
				snapshot,
				c.address,
			)
			return
		}

		snapshots, err := nsp.GetSnapshots(c.folderPath, true)
		if err != nil {
			t.Errorf("Cannot get '%s' snapshot list: %v", c.folderPath, err)
			return
		} else if len(snapshots) == 0 {
			t.Errorf(
				"New snapshot '%s' was not found in '%s' snapshot list, the list is empty: %v",
				c.snapshotName,
				c.folderPath,
				snapshots,
			)
			return
		} else if !snapshotArrayContains(snapshots, testSnapshotPath) {
			t.Errorf(
				"New snapshot '%s' was not found in '%s' snapshot list: %v",
				c.snapshotName,
				c.folderPath,
				snapshots,
			)
			return
		}
	})

	t.Run("CloneSnapshot()", func(t *testing.T) {
		nsp.DestroySnapshot(testSnapshotPath)
		nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})
		nsp.DestroyFilesystem(testSnapshotCloneTargetPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})
		nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: c.folderPath})

		err := nsp.CreateSnapshot(ns.CreateSnapshotParams{Path: testSnapshotPath})
		if err != nil {
			t.Error(err)
			return
		}

		err = nsp.CloneSnapshot(testSnapshotPath, ns.CloneSnapshotParams{
			TargetPath: testSnapshotCloneTargetPath,
		})
		if err != nil {
			t.Error(err)
			return
		}

		_, err = nsp.GetFilesystem(testSnapshotCloneTargetPath)
		if err != nil {
			t.Errorf("Cannot get created filesystem '%s': %v", testSnapshotCloneTargetPath, err)
			return
		}
	})

	t.Run("DestroySnapshot()", func(t *testing.T) {
		nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})
		nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: c.folderPath})
		nsp.CreateSnapshot(ns.CreateSnapshotParams{Path: testSnapshotPath})

		err := nsp.DestroySnapshot(testSnapshotPath)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("DestroyFilesystem() for filesystem with snapshots", func(t *testing.T) {
		nsp.DestroySnapshot(testSnapshotPath)
		nsp.DestroyFilesystem(testSnapshotCloneTargetPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})
		nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})

		err := nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: c.folderPath})
		if err != nil {
			t.Errorf("Failed to create preconditions: Create filesystem '%s' failed: %v", c.folderPath, err)
			return
		}
		err = nsp.CreateSnapshot(ns.CreateSnapshotParams{Path: testSnapshotPath})
		if err != nil {
			t.Errorf("Failed to create preconditions: Create snapshot '%s' failed: %v", testSnapshotPath, err)
			return
		}

		err = nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{DestroySnapshots: false})
		if !ns.ErrorZebiInUse(err) {
			t.Errorf(
				`Filesystem delete request is supposed to return EZEBI_RESOURCE_INUSE error in case of deleting
				filesystem with snapshots, but it's not: %v`,
				err,
			)
			return
		}

		err = nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{DestroySnapshots: true})
		if err != nil {
			t.Errorf("Cannot destroy filesystem, even with snapshots=true option: %v", err)
			return
		}

		filesystem, err := nsp.GetFilesystem(c.folderPath)
		if !ns.ErrorZebiResourceNotFound(err) {
			t.Errorf(
				"Get filesystem request should return ENOENT error, but it returns filesystem: %v, error: %v",
				filesystem,
				err,
			)
		}
	})

	t.Run("DestroyFilesystem() for filesystem with clones", func(t *testing.T) {
		nsp.DestroySnapshot(testSnapshotPath)
		nsp.DestroyFilesystem(testSnapshotCloneTargetPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})
		nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})

		err := nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: c.folderPath})
		if err != nil {
			t.Errorf("Failed to create preconditions: Create filesystem '%s' failed: %v", c.folderPath, err)
			return
		}
		err = nsp.CreateSnapshot(ns.CreateSnapshotParams{Path: testSnapshotPath})
		if err != nil {
			t.Errorf("Failed to create preconditions: Create snapshot '%s' failed: %v", testSnapshotPath, err)
			return
		}
		err = nsp.CloneSnapshot(testSnapshotPath, ns.CloneSnapshotParams{
			TargetPath: testSnapshotCloneTargetPath,
		})
		if err != nil {
			t.Errorf(
				"Failed to create preconditions: Create clone '%s' of '%s' failed: %v",
				testSnapshotCloneTargetPath,
				testSnapshotPath,
				err,
			)
			return
		}

		err = nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})
		if err != nil {
			t.Errorf("Cannot destroy filesystem: %v", err)
			return
		}

		filesystem, err := nsp.GetFilesystem(c.folderPath)
		if !ns.ErrorZebiResourceNotFound(err) {
			t.Errorf(
				"Get filesystem request should return ENOENT error, but it returns filesystem: %v, error: %v",
				filesystem,
				err,
			)
		}

		filesystem, err = nsp.GetFilesystem(testSnapshotCloneTargetPath)
		if err != nil {
			t.Errorf(
				"Cloned filesystem '%s' should be presented, but there is an error while getting it: %v",
				testSnapshotCloneTargetPath,
				err,
			)
		}
	})

	t.Run("GetFilesystemAvailableCapacity()", func(t *testing.T) {
		nsp.DestroyFilesystem(c.folderPath, ns.DestroyFilesystemParams{
			DestroySnapshots:               true,
			PromoteMostRecentCloneIfExists: true,
		})

		var referencedQuotaSize int64 = 3 * 1024 * 1024 * 1024

		err = nsp.CreateFilesystem(ns.CreateFilesystemParams{
			Path:                c.folderPath,
			ReferencedQuotaSize: referencedQuotaSize,
		})
		if err != nil {
			t.Error(err)
			return
		}

		availableCapacity, err := nsp.GetFilesystemAvailableCapacity(c.folderPath)
		if err != nil {
			t.Error(err)
			return
		} else if availableCapacity == 0 {
			t.Errorf("New filesystem %s indicates wrong available capacity (0), on: %s", c.folderPath, c.address)
		} else if availableCapacity > referencedQuotaSize {
			t.Errorf(
				"New filesystem %s available capacity expected to be more or equal to %d, but got %d (NS %s)",
				c.folderPath,
				referencedQuotaSize,
				availableCapacity,
				c.address,
			)
		}
	})

	t.Run("GetFilesystemsSlice()", func(t *testing.T) {
		err := cleanupProject(nsp, c.projectPath)

		if err != nil {
			t.Error(err)
			return
		}

		//err = nsp.CreateFilesystem(ns.CreateFilesystemParams{
		//	Path: c.folderPath,
		//})
		//if err != nil {
		//	t.Error(err)
		//	return
		//}

		count := 10
		err = createFilesystemChildren(nsp, c.projectPath, count)
		if err != nil {
			t.Error(err)
			return
		}

		filesystems, err := nsp.GetFilesystemsSlice(c.projectPath, 0, 0)
		if err == nil {
			t.Errorf("Should return an error when limit is equal 0, but got: %v", err)
			return
		}

		filesystems, err = nsp.GetFilesystemsSlice(c.projectPath, 2, 0)
		if err != nil {
			t.Error(err)
			return
		} else if len(filesystems) != 2 {
			t.Errorf("GetFilesystems() returned %d filesystems, but expected 2", len(filesystems))
			return
		} else if filesystems[0].Path != getFilesystemChildName(c.projectPath, 1) {
			t.Errorf(
				"Limit: '2', offset: '0' - first item expected to be '%s' but got: %+v",
				getFilesystemChildName(c.projectPath, 1),
				filesystems,
			)
			return
		} else if filesystems[1].Path != getFilesystemChildName(c.projectPath, 2) {
			t.Errorf(
				"Limit: '2', offset: '0' - second item expected to be '%s' but got: %+v",
				getFilesystemChildName(c.projectPath, 2),
				filesystems,
			)
			return
		}

		filesystems, err = nsp.GetFilesystemsSlice(c.projectPath, 4, 3)
		if err != nil {
			t.Error(err)
			return
		} else if len(filesystems) != 4 {
			t.Errorf("Returned %d filesystems, but expected 4", len(filesystems))
			return
		} else if filesystems[0].Path != getFilesystemChildName(c.projectPath, 4) {
			t.Errorf(
				"Limit: '4', offset: '3' - first item expected to be '%s' but got: %+v",
				getFilesystemChildName(c.projectPath, 4),
				filesystems,
			)
			return
		} else if filesystems[1].Path != getFilesystemChildName(c.projectPath, 5) {
			t.Errorf(
				"Limit: '4', offset: '3' - second item expected to be '%s' but got: %+v",
				getFilesystemChildName(c.projectPath, 5),
				filesystems,
			)
			return
		} else if filesystems[2].Path != getFilesystemChildName(c.projectPath, 6) {
			t.Errorf(
				"Limit: '4', offset: '3' - third item expected to be '%s' but got: %+v",
				getFilesystemChildName(c.projectPath, 6),
				filesystems,
			)
			return
		}
	})

	t.Run("GetFilesystems() pagination", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping pagination test in short mode")
			return
		}

		err := cleanupProject(nsp, c.projectPath)

		if err != nil {
			t.Error(err)
			return
		}

		count := 19
		t.Logf("create %d children filesystems", count)
		err = createFilesystemChildren(nsp, c.projectPath, count)
		if err != nil {
			t.Error(err)
			return
		}

		t.Log("get all filesystems")
		filesystems, err := nsp.GetFilesystems(c.projectPath)
		if err != nil {
			t.Error(err)
			return
		} else if len(filesystems) != count {
			t.Errorf("GetFilesystems() returned %d filesystems, but expected %d", len(filesystems), count)
		}

		t.Log("check if all filesystems are in the list")
		for i := 1; i <= len(filesystems); i++ {
			fs := getFilesystemChildName(c.projectPath, i)
			if !filesystemArrayContains(filesystems, fs) {
				t.Errorf("Filesystem list doesn't contain '%s' filesystem", fs)
			}
		}
	})

	t.Run("GetFilesystemsWithStartingToken() pagination", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping pagination test in short mode")
			return
		}

		err := cleanupProject(nsp, c.projectPath)

		if err != nil {
			t.Error(err)
			return
		}

		count := 25
		t.Logf("create %d children filesystems", count)
		if err = createFilesystemChildren(nsp, c.projectPath, count); err != nil {
			t.Error(err)
			return
		}

		tokenTests := []struct {
			Limit             int
			ExpectedCount     int
			StartingToken     string
			ExpectedNextToken string
		}{
			{count + 1, count, "", ""},
			{5, 5, "", getFilesystemChildName(c.projectPath, 5)},
			{5, 5, getFilesystemChildName(c.projectPath, 5), getFilesystemChildName(c.projectPath, 10)},
			{10, 10, getFilesystemChildName(c.projectPath, 3), getFilesystemChildName(c.projectPath, 13)},
			{10, 10, getFilesystemChildName(c.projectPath, 10), getFilesystemChildName(c.projectPath, 20)},
			{1, 0, "NOT_EXISTING", ""},
			{0, count, "", ""},
		}

		for _, v := range tokenTests {
			f := fmt.Sprintf("startingToken: '%s', limit: '%d'", v.StartingToken, v.Limit)
			t.Logf("...check %s", f)

			filesystems, nextToken, err := nsp.GetFilesystemsWithStartingToken(c.projectPath, v.StartingToken, v.Limit)
			if err != nil {
				t.Error(err)
				return
			} else if len(filesystems) != v.ExpectedCount {
				t.Errorf("%s: returned %d filesystems, but expected %d", f, len(filesystems), v.ExpectedCount)
				return
			} else if nextToken != v.ExpectedNextToken {
				t.Errorf("%s: returned '%s' next token, but expected '%s'", f, nextToken, v.ExpectedNextToken)
			}
		}

		t.Log("get all filesystems using tokens")
		nextToken := ""
		filesystems := []ns.Filesystem{}
		for {
			filesystemsSlice, nt, err := nsp.GetFilesystemsWithStartingToken(c.projectPath, nextToken, 25)
			if err != nil {
				t.Error(err)
				return
			}

			filesystems = append(filesystems, filesystemsSlice...)
			if len(filesystems) > count {
				t.Errorf(
					"Get all filesystems operation is expected to return %d filesystems, but already got %d; Cancel.",
					count,
					len(filesystems),
				)
				return
			}

			if nt == "" {
				break
			} else {
				nextToken = nt
			}
		}

		if len(filesystems) != count {
			t.Errorf("Get all filesystems operation returned %d filesystems but expected %d", len(filesystems), count)
			return
		}

		t.Log("check if all filesystems are in the list")
		for i := 1; i <= len(filesystems); i++ {
			fs := getFilesystemChildName(c.projectPath, i)
			if !filesystemArrayContains(filesystems, getFilesystemChildName(c.projectPath, i)) {
				t.Errorf("Filesystem list doesn't contain '%s' filesystem", fs)
			}
		}
	})

	// clean up
	cleanupProject(nsp, c.projectPath)
	err = nsp.CreateFilesystem(ns.CreateFilesystemParams{
		Path: c.folderPath,
	})
	if err != nil {
		t.Error(err)
		return
	}
	// nsp.DestroySnapshot(testSnapshotPath)
	// destroyFilesystemWithDependents(nsp, testSnapshotCloneTargetPath)
	// destroyFilesystemWithDependents(nsp, c.folderPath)
}

// getFilesystemChildName("fs", 13) === "fs/child-013"
func getFilesystemChildName(parent string, id int) string {
	return path.Join(parent, fmt.Sprintf("child-%03d", id))
}

func createFilesystemChildren(nsp ns.ProviderInterface, parent string, count int) error {
	jobs := make([]func() error, count)
	for i := 0; i < count; i++ {
		i := i
		jobs[i] = func() error {
			return nsp.CreateFilesystem(ns.CreateFilesystemParams{Path: getFilesystemChildName(parent, i+1)})
		}
	}

	return runConcurrentJobs("create filesystem", jobs)
}

func cleanupProject(nsp ns.ProviderInterface, path string) error {
	nsp.DeleteProject(c.projectPath)
	for {
		_, err := nsp.GetProject(c.projectPath)
		if err != nil {
			break
		}
		time.Sleep(time.Second)
	}
	return nsp.CreateProject(c.projectPath)
}

func destroyFilesystemWithDependents(nsp ns.ProviderInterface, filesystem string) error {
	err := nsp.DestroyFilesystem(filesystem, ns.DestroyFilesystemParams{DestroySnapshots: true})
	if err != nil {
		return fmt.Errorf("destroyFilesystemWithDependents(%s): failed to destroy filesystem: %v", filesystem, err)
	}

	return nil
}

func runConcurrentJobs(description string, jobs []func() error) error {
	count := len(jobs)

	worker := func(jobsPool <-chan func() error, results chan<- error) {
		for job := range jobsPool {
			err := job()
			if err != nil {
				results <- fmt.Errorf("Job failed: %s: %s", description, err)
			} else {
				results <- nil
			}
		}
	}

	jobsPool := make(chan func() error, count)
	results := make(chan error, count)

	// start workers
	for i := 0; i < concurrentProcesses; i++ {
		go worker(jobsPool, results)
	}

	// schedule jobs
	for _, job := range jobs {
		jobsPool <- job
	}
	close(jobsPool)

	// collect all results
	errors := []error{}
	for i := 0; i < count; i++ {
		err := <-results
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		err := ""
		for _, e := range errors {
			err += fmt.Sprintf("\n%s;", e)
		}
		return fmt.Errorf("%d of %d jobs failed: %s: %s", len(errors), count, description, err)
	}

	return nil
}

func poolArrayContains(array []ns.Pool, value string) bool {
	for _, v := range array {
		if v.Name == value {
			return true
		}
	}
	return false
}

func filesystemArrayContains(array []ns.Filesystem, value string) bool {
	for _, v := range array {
		if v.Path == value {
			fmt.Println(" =====> FOUND", value)
			return true
		} else {
			fmt.Println(" =====> v.Path", v.Path, " != ", value)
		}
	}
	return false
}

func snapshotArrayContains(array []ns.Snapshot, value string) bool {
	for _, v := range array {
		if v.Path == value {
			return true
		}
	}
	return false
}

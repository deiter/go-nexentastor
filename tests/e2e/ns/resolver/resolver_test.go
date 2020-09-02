package resolver_test

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/Nexenta/go-nexentastor/pkg/ns"
)

const (
	defaultUsername    = "admin"
	defaultPassword    = "t"
	defaultPoolName    = "pool-a"
	defaultProjectName = "csi"
	defaultFolderName  = "folder"
)

type config struct {
	address  string
	username string
	password string
	pool     string
	project  string
	folder   string
}

var c *config
var l *logrus.Entry

func filesystemArrayContains(array []ns.Filesystem, value string) bool {
	for _, v := range array {
		if v.Path == value {
			return true
		}
	}
	return false
}

func TestMain(m *testing.M) {
	var (
		address  = flag.String("address", "", "NS API [schema://host:port,...]")
		username = flag.String("username", defaultUsername, "overwrite NS API username from config")
		password = flag.String("password", defaultPassword, "overwrite NS API password from config")
		pool     = flag.String("pool", defaultPoolName, "pool on NS")
		project  = flag.String("project", defaultProjectName, "project on NS")
		folder   = flag.String("folder", defaultFolderName, "folder on NS")
		log      = flag.Bool("log", false, "show logs")
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
		address:  *address,
		username: *username,
		password: *password,
		pool:     *pool,
		project:  fmt.Sprintf("%s/Local/%s", *pool, *project),
		folder:   fmt.Sprintf("%s/Local/%s/%s", *pool, *project, *folder),
	}

	os.Exit(m.Run())
}

func TestResolver_NewResolverMulti(t *testing.T) {
	t.Logf("Using NS: %s", c.address)

	nsr, err := ns.NewResolver(ns.ResolverArgs{
		Address:            c.address,
		Username:           c.username,
		Password:           c.password,
		Log:                l,
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Error(err)
		return
	}

	t.Run("Resolve() should return NS with requested dataset", func(t *testing.T) {
		//err = ns.CreateProject(c.project)
		//if err != nil {
		//	t.Error(err)
		//	return
		//}

		//parameters := ns.CreateFilesystemParams {
		//	Path: c.folder,
		//}

		//err = nsProvider.CreateFilesystem(parameters)
		//if err != nil {
		//	t.Error(err)
		//	return
		//}

		nsProvider, err := nsr.Resolve(c.project)
		if err != nil {
			t.Error(err)
			return
		} else if nsProvider == nil {
			t.Error("No NS returned by resolver")
			return
		}

		filesystems, err := nsProvider.GetFilesystems(c.project)
		if err != nil {
			t.Errorf("NS Error: %s", err)
			return
		} else if !filesystemArrayContains(filesystems, c.folder) {
			t.Errorf("Returned NS (%s) doesn't contain dataset: %s", nsProvider, c.folder)
			return
		}
	})

	t.Run("Resolve() should return error if dataset not exists", func(t *testing.T) {
		nsProvider, err := nsr.Resolve("not/exists")
		if err == nil {
			t.Errorf("Resolver return NS for non-existing datastore: %s", nsProvider)
			return
		}
	})
}

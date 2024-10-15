package main

import (
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"github.com/lonng/nano"
	"github.com/lonng/nano/cluster"
	"github.com/lonng/nano/examples/cluster/chat"
	"github.com/lonng/nano/examples/cluster/gate"
	"github.com/lonng/nano/examples/cluster/master"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/serialize/json"
	"github.com/lonng/nano/session"
	"github.com/pingcap/errors"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "NanoClusterDemo"
	app.Author = "Lonng"
	app.Email = "heng@lonng.org"
	app.Description = "Nano cluster demo"
	app.Commands = []cli.Command{
		{
			Name: "master",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "listen,l",
					Usage: "Master service listen address",
					Value: "127.0.0.1:34567",
				},
			},
			Action: runMaster,
		},
		{
			Name: "gate",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "master",
					Usage: "master server address",
					Value: "127.0.0.1:34567",
				},
				cli.StringFlag{
					Name:  "listen,l",
					Usage: "Gate service listen address",
					Value: "",
				},
				cli.StringFlag{
					Name:  "gate-address",
					Usage: "Client connect address",
					Value: "",
				},
			},
			Action: runGate,
		},
		{
			Name: "chat",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "master",
					Usage: "master server address",
					Value: "127.0.0.1:34567",
				},
				cli.StringFlag{
					Name:  "listen,l",
					Usage: "Chat service listen address",
					Value: "",
				},
			},
			Action: runChat,
		},
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Startup server error %+v", err)
	}
}

func srcPath() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}

func runMaster(args *cli.Context) error {
	listen := args.String("listen")
	if listen == "" {
		return errors.Errorf("master listen address cannot empty")
	}

	webDir := filepath.Join(srcPath(), "master", "web")
	log.Println("Nano master server web content directory", webDir)
	log.Println("Nano master listen address", listen)
	log.Println("Open http://127.0.0.1:12345/web/ in browser")

	http.Handle("/web/", http.StripPrefix("/web/", http.FileServer(http.Dir(webDir))))
	go func() {
		if err := http.ListenAndServe(":12345", nil); err != nil {
			panic(err)
		}
	}()

	// Register session closed callback
	session.Lifetime.OnClosed(master.OnSessionClosed)

	// Startup Nano server with the specified listen address
	nano.Listen(listen,
		nano.WithMaster(),
		nano.WithComponents(master.Services),
		nano.WithSerializer(json.NewSerializer()),
		nano.WithDebugMode(),
		nano.WithLabel("master"),
		nano.WithUnregisterCallback(func(m cluster.Member, _ func()) {
			log.Println("Todo alarm unregister:", m.String())
		}),
	)

	return nil
}

var runTimes int

func runGate(args *cli.Context) error {
	listen := args.String("listen")
	if listen == "" {
		return errors.Errorf("gate listen address cannot empty")
	}

	masterAddr := args.String("master")
	if masterAddr == "" {
		return errors.Errorf("master address cannot empty")
	}

	gateAddr := args.String("gate-address")
	if gateAddr == "" {
		return errors.Errorf("gate address cannot empty")
	}

	log.Println("Current server listen address", listen)
	log.Println("Current gate server address", gateAddr)
	log.Println("Remote master server address", masterAddr)

	pip := pipeline.New()

	pip.Inbound().PushBack(func(s *session.Session, msg *pipeline.Message) error {
		runTimes++
		log.Println("Inbound", runTimes, string(s.UID()))
		if runTimes < rand.Intn(3)+1 {
			log.Println("Closing session due to random check")
			// s.Close()
			return nil
		}
		s.SetClientUid(9999)
		return nil
	})

	// go func() {
	// 	for {
	// 		time.Sleep(time.Second * 2)
	// 		labels := []string{"chat"}
	// 		lives, dies, err := nano.PingNodes(labels)
	// 		if err != nil {
	// 			log.Println("PingNodes error", err)
	// 		}
	// 		log.Println("PingNodes", lives, dies)
	// 	}
	// }()

	// Startup Nano server with the specified listen address
	nano.Listen(listen,
		nano.WithAdvertiseAddr(masterAddr),
		nano.WithClientAddr(gateAddr),
		nano.WithComponents(gate.Services),
		nano.WithPipeline(pip),
		nano.WithSerializer(json.NewSerializer()),
		nano.WithIsWebsocket(true),
		nano.WithWSPath("/nano"),
		nano.WithCheckOriginFunc(func(_ *http.Request) bool { return true }),
		nano.WithDebugMode(),
		nano.WithLabel("gate"),
		nano.WithNodeId(2), // if you deploy multi gate, option set nodeId, default nodeId = os.Getpid()
	)
	return nil
}

func runChat(args *cli.Context) error {
	listen := args.String("listen")
	if listen == "" {
		return errors.Errorf("chat listen address cannot empty")
	}

	masterAddr := args.String("master")
	if listen == "" {
		return errors.Errorf("master address cannot empty")
	}

	log.Println("Current chat server listen address", listen)
	log.Println("Remote master server address", masterAddr)

	// Register session closed callback
	session.Lifetime.OnClosed(chat.OnSessionClosed)

	// Startup Nano server with the specified listen address
	nano.Listen(listen,
		nano.WithAdvertiseAddr(masterAddr),
		nano.WithComponents(chat.Services),
		nano.WithSerializer(json.NewSerializer()),
		nano.WithLabel("chat"),
		nano.WithDebugMode(),
	)

	return nil
}

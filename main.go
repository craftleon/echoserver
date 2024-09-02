package main

import (
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/urfave/cli/v2"

	"echoserver/server"
)

func main() {
	app := cli.NewApp()
	app.Name = "echoserver"
	app.Usage = "return layer4 5-element-tuple of the incoming packet"

	runCmd := &cli.Command{
		Name:  "run",
		Usage: "create and run echo server",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "a", Value: "", Usage: "listening interface address for the server, leave it empty for all interfaces"},
			&cli.IntFlag{Name: "p", Value: 57575, Usage: "listening port for the server, default 57575"},
		},
		Action: func(c *cli.Context) error {
			return runApp(c.String("a"), c.Int("p"))
		},
	}

	app.Commands = []*cli.Command{
		runCmd,
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}

func runApp(addr string, port int) error {
	exeFilePath, err := os.Executable()
	if err != nil {
		return err
	}
	exeDirPath := filepath.Dir(exeFilePath)
	server.ExeDirPath = exeDirPath

	ts := server.TcpServer{}
	us := server.UdpServer{}
	err = ts.Start(addr, port)
	if err != nil {
		return err
	}
	err = us.Start(addr, port)
	if err != nil {
		return err
	}

	// react to terminate signals
	termCh := make(chan os.Signal, 1)
	signal.Notify(termCh, syscall.SIGTERM, os.Interrupt)

	// block until terminated
	<-termCh
	ts.Stop()
	us.Stop()

	return nil
}

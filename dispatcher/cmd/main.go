package main

import (
	"flag"
	"github.com/puper/golang-job-dispatcher/dispatcher"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var configFile = flag.String("config", "/etc/golang-job-dispatcher/config.json", "config file")

func main() {
	flag.Parse()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := dispatcher.NewConfigWithFile(*configFile)
	if err != nil {
		log.Println(err)
		return
	}
	d, err := dispatcher.NewDispatcher(cfg)
	if err != nil {
		log.Println(err)
		return
	}
	println("start...")
	go d.Start()
	<-sc
	d.Close()
}

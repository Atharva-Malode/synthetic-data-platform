package main

import (
	"flag"
	"log"

	"github.com/dicedb/dice/config"
	"github.com/dicedb/dice/server"
)

func setupFlags() {
	flag.StringVar(&config.Host, "host", "0.0.0.0", "Host to listen on")
	flag.IntVar(&config.HTTPPort, "port", 8080, "HTTP API port")
	flag.BoolVar(&config.MCP, "mcp", false, "Run as an MCP stdio server")
	flag.Parse()
}

func main() {
	setupFlags()
	if config.MCP {
		if err := server.RunMCPServer(); err != nil {
			log.Fatal(err)
		}
		return
	}
	log.Println("Starting synthetic data API...")
	if err := server.RunHTTPServer(); err != nil {
		log.Fatal(err)
	}
}

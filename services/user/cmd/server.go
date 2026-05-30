package cmd

import (
	"fmt"
	"net/http"
)

type Server struct {
	httpServer *http.Server
}

func NewServer() *Server {
	httpServer := &http.Server{
		Addr: fmt.Sprintf("%s:%d"),
	}

	return &Server{httpServer: httpServer}
}

func (server *Server) Run() error {
	return server.httpServer.ListenAndServe()
}

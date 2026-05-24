package api

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type Server struct {
	addr string
	http *http.Server
	h    *Handlers
}

func NewServer(addr string, handlers *Handlers) *Server {
	return &Server{
		addr: addr,
		http: &http.Server{
			Addr:              addr,
			Handler:           handlers.Routes(),
			ReadHeaderTimeout: 5 * time.Second,
		},
		h: handlers,
	}
}

func (s *Server) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.http.Shutdown(shutdownCtx)
	}()
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

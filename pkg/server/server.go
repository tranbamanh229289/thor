package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"thor/pkg/config"

	"go.uber.org/zap"
)

type Server struct {
	httpServer *http.Server
	log        *zap.Logger
	cfg        *config.HTTPServerConfig
}

func New(cfg *config.HTTPServerConfig, log *zap.Logger) *Server {
	httpServer := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:      nil,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}
	if cfg.TLS.Enable {
		httpServer.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			MaxVersion: tls.VersionTLS13,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			},
			CurvePreferences: []tls.CurveID{
				tls.X25519,
				tls.CurveP256,
				tls.CurveP384,
			},
			SessionTicketsDisabled: false,
			NextProtos:             []string{"h2", "http/1.1"},
			ClientAuth:             tls.NoClientCert,
			Renegotiation:          tls.RenegotiateNever,
		}
	}

	return &Server{
		httpServer: httpServer,
		log:        log,
		cfg:        cfg,
	}
}

func (s *Server) Run() error {
	errCh := make(chan error, 1)

	go func() {
		s.log.Info("Server starting: ", zap.String("addr:", s.httpServer.Addr))
		var err error

		if s.cfg.TLS.Enable {
			s.log.Info("TLS enabled")
			err = s.httpServer.ListenAndServeTLS(s.cfg.TLS.CertFile, s.cfg.TLS.KeyFile)
		} else {
			s.log.Info("TLS disabled")
			err = s.httpServer.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return fmt.Errorf("Server error: %w", err)
	case sig := <-quit:
		s.log.Info("Shutdown signal received", zap.String("signal", sig.String()))
	}

	return s.Shutdown()
}

func (s *Server) SetHandler(httpHandler *http.Handler) {
	s.httpServer.Handler = *httpHandler
}

func (s *Server) Shutdown() error {
	shutDownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	if err := s.httpServer.Shutdown(shutDownCtx); err != nil {
		return fmt.Errorf("Server shutdown error: %w", err)
	}
	s.log.Info("Server stopped")
	return nil
}

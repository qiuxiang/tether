package hub

import (
	"net/http"
)

type Options struct {
	Token string
}

type Server struct {
	opts     Options
	registry *Registry
}

func NewServer(opts Options) *Server {
	return &Server{opts: opts, registry: NewRegistry()}
}

func (s *Server) Registry() *Registry { return s.registry }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/device", s.handleDevice)
	return mux
}

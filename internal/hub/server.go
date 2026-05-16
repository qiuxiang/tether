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
	router   *Router
}

func NewServer(opts Options) *Server {
	return &Server{opts: opts, registry: NewRegistry(), router: NewRouter()}
}

func (s *Server) Registry() *Registry { return s.registry }

func (s *Server) Router() *Router { return s.router }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/device", s.handleDevice)
	mcpH := s.authMCP(s.mcpHandler())
	mux.Handle("/mcp", mcpH)
	mux.Handle("/mcp/", mcpH)
	return mux
}

// mcpHandler is a stub. The real implementation lives in mcp.go; that file is
// temporarily excluded via //go:build ignore until Task 4 deletes it.
func (s *Server) mcpHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "mcp not available", http.StatusNotImplemented)
	})
}

func (s *Server) authMCP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		want := "Bearer " + s.opts.Token
		if h != want {
			http.Error(w, "unauthorized", 401)
			return
		}
		next.ServeHTTP(w, r)
	})
}

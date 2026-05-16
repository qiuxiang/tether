package hub

import "net/http"

type Options struct {
	Token string
}

type Server struct {
	opts     Options
	registry *Registry
	clients  *ClientRegistry
	router   *Router
}

func NewServer(opts Options) *Server {
	return &Server{
		opts:     opts,
		registry: NewRegistry(),
		clients:  NewClientRegistry(),
		router:   NewRouter(),
	}
}

func (s *Server) Registry() *Registry      { return s.registry }
func (s *Server) Clients() *ClientRegistry { return s.clients }
func (s *Server) Router() *Router          { return s.router }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/device", s.handleDevice)
	mux.HandleFunc("/client", s.handleClient)
	return mux
}

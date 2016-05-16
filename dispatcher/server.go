package dispatcher

import (
	"log"
	"net/http"
	"strconv"
)

type Server struct {
	Host       string
	Port       int
	dispatcher *Dispatcher
}

func NewServer(host string, port int, dispatcher *Dispatcher) *Server {
	return &Server{
		Host:       host,
		Port:       port,
		dispatcher: dispatcher,
	}
}

func (this *Server) Put(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	typ := r.FormValue("type")
	data := r.FormValue("data")

	job, err := this.dispatcher.Put(&Job{
		Type: typ,
		Key:  key,
		Data: data,
	})
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		log.Println(err.Error())
		w.Write([]byte(`{"error":1}`))
		return
	}
	w.Write([]byte(`{"error":0, "result": {"id": ` + strconv.Itoa(int(job.Id)) + `}}")`))
}

func (this *Server) Pause(w http.ResponseWriter, r *http.Request) {
	this.dispatcher.Pause()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"error":0}`))
}

func (this *Server) Continue(w http.ResponseWriter, r *http.Request) {
	this.dispatcher.Continue()
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"error":0}`))
}

func (this *Server) Reload(w http.ResponseWriter, r *http.Request) {
	err := this.dispatcher.Reload()
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.Write([]byte(`{"error":1}`))
	} else {
		w.Write([]byte(`{"error":0}`))
	}
}

func (this *Server) Close() {
	//nothing to do
}

func (this *Server) Start() {
	http.HandleFunc("/put", this.Put)
	http.HandleFunc("/command/pause", this.Pause)
	http.HandleFunc("/command/continue", this.Continue)
	http.HandleFunc("/command/reload", this.Reload)
	http.ListenAndServe(this.Host+":"+strconv.Itoa(this.Port), nil)
}

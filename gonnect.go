package main

import (
	"github.com/gorilla/mux"
	"net/http"
	"encoding/json"
	"gonnect/connectors"
	"fmt"
	"github.com/op/go-logging"
	"flag"
)

type (
	
	gonnect struct {
		bootstrapServer string
		connectors []connectors.Connector
	}

	connectorRequest struct {
		Id string
		Type string
		KafkaTopic string
		Configuration map[string]interface{}
	}
	
)

var (
	log = logging.MustGetLogger("Gonnect")
)

func Gonnect(bootstrapServer string) (*gonnect) {
	var g gonnect
	g.bootstrapServer = bootstrapServer
	g.connectors = []connectors.Connector{}
	return &g
}

func (g *gonnect) listConnectors(w http.ResponseWriter, r *http.Request) {
	log.Info(fmt.Sprintf("Connector list was requested. (Total amount %d)", len(g.connectors)))
	json.NewEncoder(w).Encode(g.connectors)
}

func (g *gonnect) showConnector(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	for _, c := range g.connectors {
		if c.GetId() == params["id"] {
			json.NewEncoder(w).Encode(c)
			return
		}
	}
	w.WriteHeader(404)
}

func (g *gonnect) addConnector(w http.ResponseWriter, r *http.Request) {
	log.Info("Adding new connector")
	var req connectorRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Type == "mqttConnector" {
		log.Info("New MQTT connector")


		c, err := connectors.CreateMqttConnection(req.Id, g.bootstrapServer, req.KafkaTopic, req.Configuration)
		if err != nil {
			log.Error(fmt.Sprintf("An error happened creating the MQTT Connector: %s", err.Error()))
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
		} else {
			log.Info("MQTT Connector created successfully.")
			w.WriteHeader(200)
			g.connectors = append(g.connectors, connectors.Connector(*c))
			log.Info(fmt.Sprintf("Gonnect instance now runs %d connectors.", len(g.connectors)))
		}
	} else {
		log.Info(fmt.Sprintf("Unrecognized connector type: %s", req.Type))
		w.WriteHeader(501)
		w.Write([]byte(fmt.Sprintf("Connector %s not implemented", req.Type)))
	}
}

func (g *gonnect) removeConnector(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	log.Info(fmt.Sprintf("Removing connector %s", params["id"]))
	for i, c := range g.connectors {
		if c.GetId() == params["id"] {
			c.Close()
			g.connectors = append(g.connectors[:i], g.connectors[i+1:]...)
			w.WriteHeader(204)
			return
		}
	}
	w.WriteHeader(404)
}

func (g *gonnect) Run() {
	router := mux.NewRouter()
	router.HandleFunc("/connectors", g.listConnectors).Methods("GET")
	router.HandleFunc("/connectors", g.addConnector).Methods("POST")
	router.HandleFunc("/connectors/{id}", g.showConnector).Methods("GET")
	router.HandleFunc("/connectors/{id}", g.removeConnector).Methods("DELETE")
	http.ListenAndServe(":8081", router)
	log.Info("Gonnect is running at localhost:8081.")
}

func main() {
	var bootstrapServers string
	flag.StringVar(&bootstrapServers, "bootstrap-servers", "localhost:9092", "Kafka broker URL")
	flag.Parse()
	log.Info(fmt.Sprintf("Settings: Bootstrap server is %s\n", bootstrapServers))
	gonnectClient := Gonnect(bootstrapServers)
	gonnectClient.Run()
}
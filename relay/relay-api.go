package relay

import (
	"github.com/gin-gonic/gin"
	"github.com/rhizomata-io/dist-daemonize/api"
	"github.com/rhizomata-io/dist-daemonize/protocol"
)

const (
	relayPath = protocol.V1Path + "/relay"
)

// API ...
type API struct {
	relay *Relay
}

//SupportAPI create new APIService and apply to api.Server
func SupportAPI(relay *Relay, apiServer *api.Server) (api *API) {
	api = &API{relay: relay}
	relayGroup := apiServer.Group(relayPath)
	{
		relayGroup.POST("/post/:jobid", api.post)
		relayGroup.PUT("/put/:jobid", api.put)
	}
	return api
}

func (api *API) post(context *gin.Context) {
	// jobid := context.Param("jobid")
}

func (api *API) put(context *gin.Context) {
	// jobid := context.Param("jobid")
}

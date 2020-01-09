package dispatch

import (
	"github.com/gin-gonic/gin"
	"github.com/rhizomata-io/dist-daemonize/api"
	"github.com/rhizomata-io/dist-daemonize/protocol"
)

const (
	dispatchPath = protocol.V1Path + "/dispatch"
)

// API ...
type API struct {
	dispatch *Dispatch
}

//SupportAPI create new APIService and apply to api.Server
func SupportAPI(dispatch *Dispatch, apiServer *api.Server) (api *API) {
	api = &API{dispatch: dispatch}
	dispatchGroup := apiServer.Group(dispatchPath)
	{
		dispatchGroup.POST("/post/:jobid", api.post)
		dispatchGroup.PUT("/put/:jobid", api.put)
	}
	return api
}

func (api *API) post(context *gin.Context) {
	// jobid := context.Param("jobid")
}

func (api *API) put(context *gin.Context) {
	// jobid := context.Param("jobid")
}

package front

import (
	"github.com/gin-gonic/gin"
	"github.com/rhizomata-io/dist-daemonize/api"
	"github.com/rhizomata-io/dist-daemonize/protocol"
)

const (
	dispatchPath = protocol.V1Path + "/dispatch"
)

// DispatchAPI ...
type DispatchAPI struct {
	dispatch *Dispatch
}

//SupportAPI create new APIService and apply to api.Server
func SupportAPI(dispatch *Dispatch, apiServer *api.Server) (api *DispatchAPI) {
	api = &DispatchAPI{dispatch: dispatch}
	discoveryGroup := apiServer.Group(dispatchPath)
	{
		discoveryGroup.POST("/get/:jobid", api.post)
		discoveryGroup.PUT("/get/:jobid", api.put)
	}
	return api
}

func (api *DispatchAPI) post(context *gin.Context) {
	// jobid := context.Param("jobid")
}

func (api *DispatchAPI) put(context *gin.Context) {
	// jobid := context.Param("jobid")
}

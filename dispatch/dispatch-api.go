package dispatch

import (
	"net/http"

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

//SupportAPI create new API and apply to api.Server
func SupportAPI(dispatch *Dispatch, apiServer *api.Server) (api *API) {
	api = &API{dispatch: dispatch}
	dispatchGroup := apiServer.Group(dispatchPath)
	{
		dispatchGroup.PUT("/put/:jobid", api.put)
		dispatchGroup.POST("/post/:jobid", api.put)
	}
	return api
}

// /api/v1/dispatch/getbyjob/:jobid
func (api *API) put(context *gin.Context) {
	jobid := context.Param("jobid")
	data, err := context.GetRawData()

	if err != nil {
		context.Status(http.StatusInternalServerError)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
		return
	}

	dataString := string(data)
	rtnData, err := api.dispatch.Put(jobid, dataString)
	if err != nil {
		context.Status(http.StatusInternalServerError)
		context.Writer.WriteString(err.Error())
		context.Writer.Flush()
	} else {
		context.Writer.Write(rtnData)
		context.Writer.Flush()
	}
}

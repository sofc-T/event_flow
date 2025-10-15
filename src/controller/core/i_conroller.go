package controller 

import "github.com/gin-gonic/gin"


// IController outlines methods for route registration:
// - Public: No authentication required.
// - Protected: Requires authentication.
// - Privileged: Requires additional role-based Authorizeation.

type IController interface {
	// RegisterPublic sets up public routes.
	RegisterPublic(route *gin.RouterGroup)

	// RegisterProtected sets up routes that require authentication.
	RegisterProtected(route *gin.RouterGroup)

	// RegisterPrivileged sets up routes with additional role-based Authorizeation.
	RegisterPrivileged(route *gin.RouterGroup)

	//RegisterEmpty
	RegisterEmpty(route *gin.RouterGroup)
}
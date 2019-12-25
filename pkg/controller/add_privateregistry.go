package controller

import (
	"github.com/GLYASAI/rainbond-operator/pkg/controller/privateregistry"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, privateregistry.Add)
}

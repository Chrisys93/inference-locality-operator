// Command epp is the llm-d router v1.0.0 endpoint picker with bucket-affinity-scorer registered: the stock runner,
// flags and plugins, plus this module's plugin.
package main

import (
	"os"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/Chrisys93/inference-locality-operator/epp/bucketaffinity"
	"github.com/llm-d/llm-d-router/cmd/epp/runner"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

func main() {
	fwkplugin.Register(bucketaffinity.Type, fwkplugin.StabilityAlpha, bucketaffinity.Factory)
	if err := runner.NewRunner().Run(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}

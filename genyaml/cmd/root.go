/*
Copyright 2018 Red Hat Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"fmt"
	"log"
	"os"
	"encoding/json"
	"strings"

	"github.com/spf13/cobra"
	"github.com/ghodss/yaml"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/api/core/v1"
)

var defaultKubeConfig = os.Getenv("HOME") + "/.kube/config"

var Namespace string
var KubeConfig string
var AppName string
var MainClass string
var AppFile string
var Arguments string
var Image string
var DriverCores float32
var DriverMem string
var DriverLabels string
var DLabels map[string]string
var ExecCores float32
var ExecMem string
var ExecLabels string
var ELabels map[string]string
var ExecInstances int32
var ProgramType string
var DVolMountStr string
var EVolMountStr string
var DVolumeMounts []v1.VolumeMount
var EVolumeMounts []v1.VolumeMount
var ServiceAccount string

var rootCmd = &cobra.Command{
	Use:   "genyaml",
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&Namespace, "namespace", "n", "default",
		"The namespace in which the SparkApplication is to be created")
	rootCmd.PersistentFlags().StringVarP(&KubeConfig, "kubeconfig", "k", defaultKubeConfig,
		"The path to the local Kubernetes configuration file")
	rootCmd.PersistentFlags().StringVarP(&AppName, "appname", "a", "sparkapp",
		"The name of your Spark application")
	rootCmd.PersistentFlags().StringVarP(&MainClass, "class", "c", "",
		"The main class for your Spark application")
	rootCmd.PersistentFlags().StringVarP(&Arguments, "args", "", "",
		"The arguments for your Spark application")
	rootCmd.PersistentFlags().StringVarP(&AppFile, "appfile", "f", "",
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().StringVarP(&Image, "image", "i", "docker.io/crobby/openshift-spark:2.3",
		"The Spark image to be used to run your program")
	rootCmd.PersistentFlags().Float32VarP(&DriverCores, "dcores", "", 0.1,
		"The number of cores to request for your driver program")
	rootCmd.PersistentFlags().StringVarP(&DriverMem, "dmem", "", "512m",
		"The amount of memory to request for your driver program")
	rootCmd.PersistentFlags().StringVarP(&DriverLabels, "dlabels", "", "",
		"A set of labels for your driver instance")
	rootCmd.PersistentFlags().Float32VarP(&ExecCores, "ecores", "", 1.0,
		"The number of cores to request for your executors")
	rootCmd.PersistentFlags().StringVarP(&ExecMem, "emem", "", "512m",
		"The amount of memory to request for your executors")
	rootCmd.PersistentFlags().StringVarP(&ExecLabels, "elabels", "", "",
		"A set of labels for your executor instances")
	rootCmd.PersistentFlags().Int32VarP(&ExecInstances, "einst", "", 1,
		"The number of executor instances to launch")
	rootCmd.PersistentFlags().StringVarP(&ProgramType, "type", "t", "Scala",
		"The type of your Spark Application (Scala, Spark, Java, R)")
	rootCmd.PersistentFlags().StringVarP(&EVolMountStr, "evol", "", "",
		"The volume mounts for your executors")
	rootCmd.PersistentFlags().StringVarP(&DVolMountStr, "dvol", "", "",
		"The volume mounts for your driver")
	rootCmd.PersistentFlags().StringVarP(&ServiceAccount, "sa", "", "spark",
		"The service account for your driver, 'spark' by default")
}


func Execute() {
	if err := rootCmd.Execute(); err != nil {
    	fmt.Fprintf(os.Stderr, "%v", err)
	}

	obj := fillObject()

	text, err := yaml.Marshal(&obj)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("\n%s\n\n", string(text))
}

func fillObject() v1alpha1.SparkApplication {
	obj := v1alpha1.SparkApplication{}
	setSimpleFields(&obj)

	// set up the fields that need a bit more love
	obj.Spec.Arguments = strings.Split(Arguments, ",") // do something better here for ints and such
	obj.Spec.Mode = v1alpha1.ClusterMode               // could make this variable as well
	obj.Spec.RestartPolicy = v1alpha1.Never            // could make this variable as well
	setMounts(&obj)
	setLabels(&obj)
	setType(&obj)
	return obj
}

func setSimpleFields(obj *v1alpha1.SparkApplication) {
	obj.ObjectMeta.Name = AppName
	obj.Spec.MainApplicationFile = &AppFile
	obj.ObjectMeta.Namespace = Namespace
	obj.Spec.Image = &Image
	obj.Spec.Driver.Cores = &DriverCores
	obj.Spec.Driver.Memory = &DriverMem
	obj.Spec.Executor.Cores = &ExecCores
	obj.Spec.Executor.Memory = &ExecMem
	obj.Spec.Driver.ServiceAccount = &ServiceAccount
	obj.Spec.Executor.Instances = &ExecInstances
}

func setType(obj *v1alpha1.SparkApplication) {
	var givenType = strings.ToLower(ProgramType)
	switch (givenType) {
	case "scala":
		obj.Spec.Type = v1alpha1.ScalaApplicationType
	case "python":
		obj.Spec.Type = v1alpha1.PythonApplicationType
	case "java":
		obj.Spec.Type = v1alpha1.JavaApplicationType
	case "r":
		obj.Spec.Type = v1alpha1.RApplicationType
	default:
		obj.Spec.Type = v1alpha1.ScalaApplicationType
	}
}

func setLabels(obj *v1alpha1.SparkApplication) {
	var dLabels map[string]string
	if len(DriverLabels) > 0 {
		err := json.Unmarshal([]byte(DriverLabels), &dLabels)
		if err != nil {
			fmt.Println(err)
		}
	}
	obj.Spec.Driver.Labels = dLabels
	var eLabels map[string]string
	if len(ExecLabels) > 0 {
		err := json.Unmarshal([]byte(ExecLabels), &eLabels)
		if err != nil {
			fmt.Println(err)
		}
	}
	obj.Spec.Executor.Labels = eLabels
}

func setMounts(obj *v1alpha1.SparkApplication) {
	var dVolMounts []v1.VolumeMount
	if len(DVolMountStr) > 0 {
		err := json.Unmarshal([]byte(DVolMountStr), &dVolMounts)
		if err != nil {
			fmt.Println(err)
		}
	}
	obj.Spec.Driver.VolumeMounts = dVolMounts
	var eVolMounts []v1.VolumeMount
	if len(EVolMountStr) > 0 {
		err := json.Unmarshal([]byte(EVolMountStr), &eVolMounts)
		if err != nil {
			fmt.Println(err)
		}
	}
	obj.Spec.Executor.VolumeMounts = eVolMounts
}
/*
Copyright 2017 Google LLC

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
var Image string
var DriverCores float32
var DriverMem string
var DriverLabels string
var ExecCores float32
var ExecMem string
var ExecLabels string
var ExecInstances int32
var ProgramType string
var DVolMountStr string
var EVolMountStr string
var DVolumeMounts []v1.VolumeMount
var EVolumeMounts []v1.VolumeMount

var rootCmd = &cobra.Command{
	Use:   "genyaml",
	Short: "genyaml is the command-line tool for generating yaml for the Spark Operator",
	Long: `genyaml is the command-line tool for generating yaml for interacting with the Spark Operator`,
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
	rootCmd.PersistentFlags().StringVarP(&AppFile, "appfile", "f", "",
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().StringVarP(&Image, "image", "i", "docker.io/crobby/openshift-spark:2.3",
		"The Spark image to be used to run your program")
	rootCmd.PersistentFlags().Float32VarP(&DriverCores, "dcores", "", 0.1,
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().StringVarP(&DriverMem, "dmem", "", "512m",
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().StringVarP(&DriverLabels, "dlabels", "", "",
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().Float32VarP(&ExecCores, "ecores", "", 1.0,
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().StringVarP(&ExecMem, "emem", "", "512m",
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().StringVarP(&ExecLabels, "elabels", "", "",
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().Int32VarP(&ExecInstances, "einst", "", 1,
		"The name of your main Spark application file")
	rootCmd.PersistentFlags().StringVarP(&ProgramType, "type", "t", "Scala",
		"The type of your Spark Application (Scala, Spark, Java, R)")
	rootCmd.PersistentFlags().StringVarP(&EVolMountStr, "evol", "", "",
		"The volume mounts for your executors")
	rootCmd.PersistentFlags().StringVarP(&DVolMountStr, "dvol", "", "",
		"The volume mounts for your driver")
}


func Execute() {
	if err := rootCmd.Execute(); err != nil {
    	fmt.Fprintf(os.Stderr, "%v", err)
	}

	obj := fillObject()

	//err := yaml.Unmarshal([]byte(data), &obj)
	//if err != nil {
	//	log.Fatalf("error: %v", err)
	//}
	//fmt.Printf("Text converted to object:\n%v\n\n", obj)

	fmt.Printf("AppName passed in was: %s\n", AppName)

	text, err := yaml.Marshal(&obj)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("Object converted to YAML:\n%s\n\n", string(text))
}

func fillObject() v1alpha1.SparkApplication {
	obj := v1alpha1.SparkApplication{}
	obj.ObjectMeta.Name = AppName
	obj.ObjectMeta.Namespace = Namespace
	obj.Spec.Type = "Scala"
	obj.Spec.Image = &Image
	obj.Spec.Driver.Cores = &DriverCores
	obj.Spec.Driver.Memory = &DriverMem
	//obj.Spec.Driver.Labels = DriverLabels
	obj.Spec.Executor.Cores = &ExecCores
	obj.Spec.Executor.Memory = &ExecMem
	//obj.Spec.Executor.Labels = ExecLabels
	obj.Spec.Executor.Instances = &ExecInstances
	obj.Spec.Type = v1alpha1.ScalaApplicationType
	var dVolMounts []v1.VolumeMount
	err := json.Unmarshal([]byte(DVolMountStr), &dVolMounts)
	fmt.Println(err)
	var eVolMounts []v1.VolumeMount
	err := json.Unmarshal([]byte(EVolMountStr), eVolMounts)
	fmt.Println(err)
	obj.Spec.Driver.VolumeMounts = dVolMounts
	obj.Spec.Executor.VolumeMounts = eVolMounts
    return obj
}
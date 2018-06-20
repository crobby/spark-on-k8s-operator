package main

import (
	"fmt"
	"log"

	"github.com/ghodss/yaml"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)


var data = `
apiVersion: "sparkoperator.k8s.io/v1alpha1"
kind: SparkApplication
metadata:
  name: spark-pi
spec:
  type: Scala
  mode: cluster
  image: "docker.io/crobby/openshift-spark:2.3"
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar"
  arguments:
    - 1000
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 0.1
    coreLimit: "200m"
    memory: "512m"
    labels:
      version: 2.3.0
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 3
    memory: "512m"
    labels:
      version: 2.3.0
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  restartPolicy: Never
`



func main() {
	obj := v1alpha1.SparkApplication{}

	err := yaml.Unmarshal([]byte(data), &obj)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("Text converted to object:\n%v\n\n", obj)

	text, err := yaml.Marshal(&obj)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("Object converted to YAML:\n%s\n\n", string(text))
}
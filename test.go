package main

import (
	"context"
	// "math"
	// "sync"
	// "time"
	"fmt"
	// "strings"
	"path/filepath"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "k8s.io/client-go/rest"
	// crd api related
	"encoding/json"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"strconv"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"

	"k8s.io/client-go/tools/clientcmd"
)

type FairnessData struct {
	placementDecision []int // Add By sqi009
	functionName string // Add By sqi009
}

var gvr = schema.GroupVersionResource{
	Group:    "placement.com",
	Version:  "v1",
	Resource: "decisions",
}

var gvk = schema.GroupVersionKind{
	Group:    "placement.com",
	Version:  "v1",
	// Kind: "decisions",
	Kind: "Decision",
}

type PlacementDecisionCRDSpec struct {
	NodeNameList string `json:"nodeNameList"`
	NumNodes int `json:"numNodes"`
}

type PlacementDecisionCRD struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec PlacementDecisionCRDSpec `json:"spec,omitempty"`
}

type PlacementDecisionCRDList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []PlacementDecisionCRD `json:"items"`
}

func getPlacementDecision(client dynamic.Interface, namespace string, name string) (*PlacementDecisionCRD, error) {
	utd, err := client.Resource(gvr).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
			return nil, err
	}
	data, err := utd.MarshalJSON()
	if err != nil {
			return nil, err
	}
	var ct PlacementDecisionCRD
	if err := json.Unmarshal(data, &ct); err != nil {
			return nil, err
	}
	return &ct, nil
}

func createPlacementDecisionCRDWithYaml(client dynamic.Interface, namespace string, yamlData string) (*PlacementDecisionCRD, error) {
	// fmt.Printf("Try to execute NewDecodingSerializer()...\n")
	// fmt.Printf("yamlData: %v\n", yamlData)
	decoder := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	obj := &unstructured.Unstructured{}
	if _, _, err := decoder.Decode([]byte(yamlData), &gvk, obj); err != nil {
			// fmt.Printf("Decode yaml fail\n")
			return nil, err
	}
	// fmt.Printf("Try to execute Create()...\n")
	utd, err := client.Resource(gvr).Namespace(namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	if err != nil {
			return nil, err
	}
	data, err := utd.MarshalJSON()
	if err != nil {
			return nil, err
	}
	var ct PlacementDecisionCRD
	if err := json.Unmarshal(data, &ct); err != nil {
			return nil, err
	}
	return &ct, nil
}

func updatePlacementDecisionCRDWithYaml(client dynamic.Interface, namespace string, yamlData string) (*PlacementDecisionCRD, error) {
	decoder := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	obj := &unstructured.Unstructured{}
	if _, _, err := decoder.Decode([]byte(yamlData), &gvk, obj); err != nil {
			return nil, err
	}

	utd, err := client.Resource(gvr).Namespace(namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	if err != nil {
			return nil, err
	}
	obj.SetResourceVersion(utd.GetResourceVersion())
	utd, err = client.Resource(gvr).Namespace(namespace).Update(context.TODO(), obj, metav1.UpdateOptions{})
	if err != nil {
			return nil, err
	}

	data, err := utd.MarshalJSON()
	if err != nil {
			return nil, err
	}
	var ct PlacementDecisionCRD
	if err := json.Unmarshal(data, &ct); err != nil {
			return nil, err
	}
	return &ct, nil
}

func createPlacementDecision(client dynamic.Interface, namespace string, yamlData string) {
	// fmt.Printf("Try to execute createPlacementDecisionCRDWithYaml()...\n")
	ct, err := createPlacementDecisionCRDWithYaml(client, namespace, yamlData)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s | %s | %v | %v\n", ct.Namespace, ct.Name, ct.Spec.NodeNameList, ct.Spec.NumNodes)
}

func updatePlacementDecision(client dynamic.Interface, namespace string, yamlData string) {
	ct, err := updatePlacementDecisionCRDWithYaml(client, namespace, yamlData)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s | %s | %v | %v\n", ct.Namespace, ct.Name, ct.Spec.NodeNameList, ct.Spec.NumNodes)
}

// func applyPlacementDecision(fairnessDataList []FairnessData, nodeName []string) () {
func applyPlacementDecision(fairnessDataList []FairnessData) () {
	if fairnessDataList == nil {
		return
	}
	fmt.Printf("creates the in-cluster config...\n")
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	// creates the in-cluster config
	// config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the client
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	// loop over the placement decision (fairnessDataList)
	for i, _ := range fairnessDataList {
		functionName := fairnessDataList[i].functionName
		placementDecision := fairnessDataList[i].placementDecision
		var nodeNameList string
		numNodes := len(placementDecision)
		for j, _ := range placementDecision {
			nodeNameList += strconv.Itoa(placementDecision[j])
			nodeNameList += "%" // use % to break node names
		}
		// Generate the Placement decision CRD
		body := `
apiVersion: "placement.com/v1"
kind: Decision
metadata:
  name: ` + functionName +
`
spec:
  nodeNameList: ` + nodeNameList +
`
  numNodes: ` + strconv.Itoa(numNodes)

		// Find the CRD for the current function
		ct, err := getPlacementDecision(client, "default", functionName)
		if err != nil {
			// panic(err)
			// fmt.Printf("Create CRD object (%v) for function-%s\n", body,functionName)
			createPlacementDecision(client, "default", body)
		} else {
			fmt.Printf("%s %s %s %d\n", ct.Namespace, ct.Name, ct.Spec.NodeNameList, ct.Spec.NumNodes)
			// Update the placement decision of the function
			updatePlacementDecision(client, "default", body)
		}
	}
}

func main() {
	fairnessData_1 := FairnessData{ // fairnessData_1
		placementDecision: []int{0, 2, 1},
		functionName: "fairness-data-1",
	}
	fairnessData_2 := FairnessData{ // fairnessData_2
		placementDecision: []int{5, 2, 3, 4},
		functionName: "fairness-data-2",
	}
	fairnessDataList := []FairnessData{fairnessData_1, fairnessData_2}
	fmt.Printf("Try to apply the placement Decision\n")
	applyPlacementDecision(fairnessDataList)
}
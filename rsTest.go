package main

import (
	"context"
	"fmt"
	// "reflect"
	// "sort"
	"strings"
	// "sync"
	// "time"
	"os"
	"path/filepath"

	// apps "k8s.io/api/apps/v1"
	// "k8s.io/api/core/v1"
	// "k8s.io/apimachinery/pkg/api/errors"
	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	// "k8s.io/apimachinery/pkg/types"
	// utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	// "k8s.io/apimachinery/pkg/util/wait"
	// appsinformers "k8s.io/client-go/informers/apps/v1"
	// coreinformers "k8s.io/client-go/informers/core/v1"
	// clientset "k8s.io/client-go/kubernetes"
	// "k8s.io/client-go/kubernetes/scheme"
	// v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	// appslisters "k8s.io/client-go/listers/apps/v1"
	// corelisters "k8s.io/client-go/listers/core/v1"
	// "k8s.io/client-go/tools/cache"
	// "k8s.io/client-go/tools/record"
	// "k8s.io/client-go/util/workqueue"
	// "k8s.io/component-base/metrics/prometheus/ratelimiter"
	// "k8s.io/klog/v2"
	// podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	// "k8s.io/kubernetes/pkg/controller"
	// "k8s.io/utils/integer"

	// "k8s.io/client-go/rest"
	"encoding/json"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/client-go/kubernetes"
	apiv1 "k8s.io/api/core/v1"
)

var gvr = schema.GroupVersionResource{
	Group:    "placement.com",
	Version:  "v1",
	Resource: "decisions",
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

func GetPlacementDecision(functionName string) (nodeName []string) {
	if functionName == "" {
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
	// creates the clientset
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// Find the CRD for the current function
	ct, err := getPlacementDecision(client, "default", functionName)
	if err != nil {
		// panic(err)
		fmt.Printf("No CRD object for function-%s\n", functionName)
	}
	fmt.Printf("%s %s %s %d\n", ct.Namespace, ct.Name, ct.Spec.NodeNameList, ct.Spec.NumNodes)

	nodeNameList := ct.Spec.NodeNameList
	numNodes := ct.Spec.NumNodes
	nodeName = make([]string, int(numNodes))
	parts := strings.Split(nodeNameList, "%")
	for i := 0; i < int(numNodes); i++ {
		nodeName[i] = parts[i]
	}
	return nodeName
}


func main() {
	fmt.Printf("creates the in-cluster config... %s\n", os.Getenv("HOME"))
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	// kubeconfig := filepath.Join("/users/sqi009", ".kube", "config")
	// fmt.Printf("kubeconfig: %v\n", kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	// creates the in-cluster config
	// config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	rsList, err := clientset.AppsV1().ReplicaSets(apiv1.NamespaceDefault).List(context.TODO(), metav1.ListOptions{})
    if err != nil {
        fmt.Printf("Could not list ReplicaSet\n")
    }

    for _, rs := range rsList.Items {
		name := rs.Name
		index := strings.LastIndex(name, "-")
		deploymentName := name[:index]
		fmt.Printf("deploymentName: %v\n", deploymentName)
        // dp, err := getDeploymentByReplicaSetName(api.NamespaceDefault, c, &rs)
        // if err != nil {
        //     logger.Fatalf("GetDeploymentByReplicaSet Error: err=%s\n", err)
        // }

        // logger.Printf("Deployment assioated with ReplicaSet: rs-name=%s, revision=%s, dp-name=%s\n",
        //     rs.Name, rs.Annotations[RevisionAnnotation], dp.Name)
    }

	functionName := "fairness-data-1"
	fmt.Printf("functionName: %v\n", functionName)
	nodeNameList := GetPlacementDecision(functionName)
	if nodeNameList != nil {
		fmt.Printf("nodeNameList: %v\n", nodeNameList)
		// successfulCreations, err = slowStartBatch(diff, controller.SlowStartInitialBatchSize, func() error {
		// 	err := rsc.podControl.CreatePodsWithControllerRef(rs.Namespace, &rs.Spec.Template, rs, metav1.NewControllerRef(rs, rsc.GroupVersionKind))
		// 	if err != nil {
		// 		if errors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
		// 			// if the namespace is being terminated, we don't have to do
		// 			// anything because any creation will fail
		// 			return nil
		// 		}
		// 	}
		// 	return err
		// })
	}
}

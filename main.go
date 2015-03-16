package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	kapi "github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	kclient "github.com/GoogleCloudPlatform/kubernetes/pkg/client"
	kutil "github.com/GoogleCloudPlatform/kubernetes/pkg/util"
)

const (
	host       = "127.0.0.1:8080"
	ns         = "default"
	apiVersion = "v1beta3"

	etcdClusterName = "foo"

	etcdClientPort = 2379
	etcdPeerPort   = 2380
)

func main() {
	kfg := &kclient.Config{
		Host:    host,
		Version: apiVersion,
	}

	kc, err := kclient.New(kfg)
	if err != nil {
		log.Fatalf("Failed initializing kubernetes client: %v", err)
	}

	ensureClientServiceExists(kc)

	svcs := ensurePeerServicesExist(kc)
	ensurePodsExist(kc, svcs)

	log.Printf("All necessary services and pods exist")
}

func ensureClientServiceExists(kc *kclient.Client) {
	manifest := buildClientServiceManifest()
	ensureServiceExists(kc, manifest)
}

func ensurePeerServicesExist(kc *kclient.Client) []kapi.Service {
	ss := make([]kapi.Service, 0, 3)
	for i := 1; i <= 3; i++ {
		manifest := buildPeerServiceManifest(strconv.Itoa(i))
		ss = append(ss, ensureServiceExists(kc, manifest))
	}
	return ss
}

func ensureServiceExists(kc *kclient.Client, manifest kapi.Service) kapi.Service {
	svc, err := kc.Services(ns).Get(manifest.ObjectMeta.Name)
	if err == nil {
		return *svc
	}

	if err := createService(kc, manifest); err != nil {
		log.Fatalf("Failed creating service: %v", err)
	}

	log.Printf("Created service %s", manifest.ObjectMeta.Name)
	return waitForServiceIP(kc, manifest.ObjectMeta.Name)
}

func buildPeerServiceManifest(instance string) kapi.Service {
	return kapi.Service{
		TypeMeta: kapi.TypeMeta{
			Kind:       "Service",
			APIVersion: apiVersion,
		},
		ObjectMeta: kapi.ObjectMeta{
			Name: nodeName(instance),
		},
		Spec: kapi.ServiceSpec{
			Port:          etcdPeerPort,
			Selector:      map[string]string{"etcd-cluster": etcdClusterName, "etcd-instance": instance},
			ContainerPort: kutil.NewIntOrStringFromString("peer"),
		},
	}
}

func buildClientServiceManifest() kapi.Service {
	return kapi.Service{
		TypeMeta: kapi.TypeMeta{
			Kind:       "Service",
			APIVersion: apiVersion,
		},
		ObjectMeta: kapi.ObjectMeta{
			Name: fmt.Sprintf("etcd-clust-%s-clients", etcdClusterName),
		},
		Spec: kapi.ServiceSpec{
			Port:          etcdClientPort,
			Selector:      map[string]string{"etcd-cluster": etcdClusterName},
			ContainerPort: kutil.NewIntOrStringFromString("client"),
		},
	}
}

func createService(kc *kclient.Client, svc kapi.Service) error {
	_, err := kc.Services(ns).Create(&svc)
	return err
}

func createPod(kc *kclient.Client, pod kapi.Pod) error {
	_, err := kc.Pods(ns).Create(&pod)
	return err
}

func waitForServiceIP(kc *kclient.Client, name string) kapi.Service {
	var s *kapi.Service
	var err error
	for {
		s, err = kc.Services(ns).Get(name)
		if err != nil {
			log.Printf("Failed getting service: %v", err)
			continue
		} else if s.Spec.PortalIP == "" {
			log.Printf("Waiting on service to get PortalIP")
			continue
		}

		break
	}

	return *s
}

func nodeName(instance string) string {
	return fmt.Sprintf("etcd-clust-%s-peer-%s", etcdClusterName, instance)
}

func ensurePodsExist(kc *kclient.Client, ss []kapi.Service) []kapi.Pod {
	peers := make(map[string]string, len(ss))
	for _, svc := range ss {
		peers[nodeName(svc.Spec.Selector["etcd-instance"])] = svc.Spec.PortalIP
	}

	pods := make([]kapi.Pod, len(ss))
	for i, svc := range ss {
		pods[i] = buildPodManifest(svc.Spec.Selector["etcd-instance"], svc.Spec.PortalIP, peers)

		_, err := kc.Pods(ns).Get(pods[i].ObjectMeta.Name)
		if err == nil {
			continue
		}

		if err := createPod(kc, pods[i]); err != nil {
			log.Fatalf("Failed creating pod: %v", err)
		}
		log.Printf("Created pod %s", pods[i].ObjectMeta.Name)
	}

	return pods
}

func buildPodManifest(instance, advertiseIP string, peers map[string]string) kapi.Pod {
	return kapi.Pod{
		TypeMeta: kapi.TypeMeta{
			Kind:       "Pod",
			APIVersion: apiVersion,
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:   nodeName(instance),
			Labels: map[string]string{"etcd-cluster": etcdClusterName, "etcd-instance": instance},
		},
		Spec: kapi.PodSpec{
			Containers: []kapi.Container{
				kapi.Container{
					Name:  "etcd",
					Image: "quay.io/coreos/etcd:v2.0.5",
					Command: []string{
						fmt.Sprintf("--name=%s", nodeName(instance)),
						fmt.Sprintf("--listen-client-urls=http://0.0.0.0:%d", etcdClientPort),
						fmt.Sprintf("--advertise-client-urls=http://%s:%d", advertiseIP, etcdClientPort),
						fmt.Sprintf("--listen-peer-urls=http://0.0.0.0:%d", etcdPeerPort),
						fmt.Sprintf("--initial-advertise-peer-urls=http://%s:%d", advertiseIP, etcdPeerPort),
						fmt.Sprintf("--initial-cluster=%s", initialClusterPeers(peers)),
						"--initial-cluster-state=new",
					},
					Ports: []kapi.Port{
						kapi.Port{Name: "client", ContainerPort: etcdClientPort},
						kapi.Port{Name: "peer", ContainerPort: etcdPeerPort},
					},
				},
			},
		},
	}
}

func initialClusterPeers(peers map[string]string) string {
	tmp := make([]string, 0, len(peers))
	for name, ip := range peers {
		tmp = append(tmp, fmt.Sprintf("%s=http://%s:2380", name, ip))
	}
	return strings.Join(tmp, ",")
}

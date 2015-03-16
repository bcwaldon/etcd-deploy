package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	kapi "github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	kclient "github.com/GoogleCloudPlatform/kubernetes/pkg/client"
	kutil "github.com/GoogleCloudPlatform/kubernetes/pkg/util"
)

const (
	ns         = "default"
	apiVersion = "v1beta3"

	etcdClientPort = 2379
	etcdPeerPort   = 2380
)

func main() {
	fs := flag.NewFlagSet("etcd-deploy", flag.ExitOnError)
	k8sEndpoint := fs.String("kubernetes", "http://127.0.0.1:8080", "Kubernetes API endpoint")
	clusterID := fs.String("cluster", "", "identifier of cluster")
	clusterSize := fs.Int("size", 3, "expected size of cluster")

	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatalf("Failed parsing flags: %v", err)
	}

	if *clusterID == "" {
		log.Fatalf("Flag required: --cluster")
	}

	kfg := &kclient.Config{
		Host:    *k8sEndpoint,
		Version: apiVersion,
	}

	kc, err := kclient.New(kfg)
	if err != nil {
		log.Fatalf("Failed initializing kubernetes client: %v", err)
	}

	d := &deployer{
		kc:    kc,
		cID:   *clusterID,
		cSize: *clusterSize,
	}

	d.Reconcile()
	log.Printf("All necessary services and pods exist")
}

type deployer struct {
	kc    *kclient.Client
	cID   string
	cSize int
}

func (d *deployer) Reconcile() error {
	d.ensureClientServiceExists()
	svcs := d.ensurePeerServicesExist()
	d.ensurePodsExist(svcs)
	return nil
}

func (d *deployer) ensureClientServiceExists() {
	manifest := buildClientServiceManifest(d.cID)
	d.ensureServiceExists(manifest)
}

func (d *deployer) ensurePeerServicesExist() []kapi.Service {
	ss := make([]kapi.Service, 0, d.cSize)
	for i := 1; i <= d.cSize; i++ {
		manifest := buildPeerServiceManifest(d.cID, strconv.Itoa(i))
		ss = append(ss, d.ensureServiceExists(manifest))
	}
	return ss
}

func (d *deployer) ensureServiceExists(manifest kapi.Service) kapi.Service {
	svc, err := d.kc.Services(ns).Get(manifest.ObjectMeta.Name)
	if err == nil {
		return *svc
	}

	if err := d.createService(manifest); err != nil {
		log.Fatalf("Failed creating service: %v", err)
	}

	log.Printf("Created service %s", manifest.ObjectMeta.Name)
	return d.waitForServiceIP(manifest.ObjectMeta.Name)
}

func (d *deployer) createService(svc kapi.Service) error {
	_, err := d.kc.Services(ns).Create(&svc)
	return err
}

func (d *deployer) waitForServiceIP(name string) kapi.Service {
	var s *kapi.Service
	var err error
	for {
		s, err = d.kc.Services(ns).Get(name)
		if err != nil {
			log.Printf("Failed getting service: %v", err)
			time.Sleep(time.Second)
			continue
		} else if s.Spec.PortalIP == "" {
			log.Printf("Waiting on service to get PortalIP")
			time.Sleep(time.Second)
			continue
		}

		break
	}

	return *s
}

func (d *deployer) ensurePodsExist(ss []kapi.Service) []kapi.Pod {
	peers := make(map[string]string, len(ss))
	for _, svc := range ss {
		peers[nodeName(d.cID, svc.Spec.Selector["etcd-instance"])] = svc.Spec.PortalIP
	}

	pods := make([]kapi.Pod, len(ss))
	for i, svc := range ss {
		pods[i] = buildPodManifest(d.cID, svc.Spec.Selector["etcd-instance"], svc.Spec.PortalIP, peers)

		_, err := d.kc.Pods(ns).Get(pods[i].ObjectMeta.Name)
		if err == nil {
			continue
		}

		if err := d.createPod(pods[i]); err != nil {
			log.Fatalf("Failed creating pod: %v", err)
		}
		log.Printf("Created pod %s", pods[i].ObjectMeta.Name)
	}

	return pods
}

func (d *deployer) createPod(pod kapi.Pod) error {
	_, err := d.kc.Pods(ns).Create(&pod)
	return err
}

func nodeName(clusterID, instance string) string {
	return fmt.Sprintf("etcd-clust-%s-peer-%s", clusterID, instance)
}

func buildPeerServiceManifest(clusterID, instance string) kapi.Service {
	return kapi.Service{
		TypeMeta: kapi.TypeMeta{
			Kind:       "Service",
			APIVersion: apiVersion,
		},
		ObjectMeta: kapi.ObjectMeta{
			Name: nodeName(clusterID, instance),
		},
		Spec: kapi.ServiceSpec{
			Port:          etcdPeerPort,
			Selector:      map[string]string{"etcd-cluster": clusterID, "etcd-instance": instance},
			ContainerPort: kutil.NewIntOrStringFromString("peer"),
		},
	}
}

func buildClientServiceManifest(clusterID string) kapi.Service {
	return kapi.Service{
		TypeMeta: kapi.TypeMeta{
			Kind:       "Service",
			APIVersion: apiVersion,
		},
		ObjectMeta: kapi.ObjectMeta{
			Name: fmt.Sprintf("etcd-clust-%s-clients", clusterID),
		},
		Spec: kapi.ServiceSpec{
			Port:          etcdClientPort,
			Selector:      map[string]string{"etcd-cluster": clusterID},
			ContainerPort: kutil.NewIntOrStringFromString("client"),
		},
	}
}

func buildPodManifest(clusterID, instance, advertiseIP string, peers map[string]string) kapi.Pod {
	return kapi.Pod{
		TypeMeta: kapi.TypeMeta{
			Kind:       "Pod",
			APIVersion: apiVersion,
		},
		ObjectMeta: kapi.ObjectMeta{
			Name:   nodeName(clusterID, instance),
			Labels: map[string]string{"etcd-cluster": clusterID, "etcd-instance": instance},
		},
		Spec: kapi.PodSpec{
			Containers: []kapi.Container{
				kapi.Container{
					Name:  "etcd",
					Image: "quay.io/coreos/etcd:v2.0.5",
					Command: []string{
						fmt.Sprintf("--name=%s", nodeName(clusterID, instance)),
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
		tmp = append(tmp, fmt.Sprintf("%s=http://%s:%d", name, ip, etcdPeerPort))
	}
	return strings.Join(tmp, ",")
}

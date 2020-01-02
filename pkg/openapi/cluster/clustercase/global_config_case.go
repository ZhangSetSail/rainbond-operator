package clustercase

import (
	"fmt"

	"github.com/GLYASAI/rainbond-operator/pkg/apis/rainbond/v1alpha1"
	"github.com/GLYASAI/rainbond-operator/pkg/generated/clientset/versioned"
	"github.com/GLYASAI/rainbond-operator/pkg/openapi/model"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// GlobalConfigCaseGatter config case gatter
type GlobalConfigCaseGatter interface {
	GlobalConfigs() GlobalConfigCase
}

// GlobalConfigCase global config case
type GlobalConfigCase interface {
	GlobalConfigs() (*model.GlobalConfigs, error)
	UpdateGlobalConfig(config *model.GlobalConfigs) error
}

// GlobalConfigCaseImpl case
type GlobalConfigCaseImpl struct {
	normalClientset *kubernetes.Clientset
	rbdClientset    *versioned.Clientset
	namespace       string
	configName      string
	etcdSecretName  string
}

// NewGlobalConfigCase new global config case
func NewGlobalConfigCase(namespace, configName, etcdSecretName string, normalClientset *kubernetes.Clientset, rbdClientset *versioned.Clientset) *GlobalConfigCaseImpl {
	return &GlobalConfigCaseImpl{
		namespace:       namespace,
		configName:      configName,
		etcdSecretName:  etcdSecretName,
		normalClientset: normalClientset,
		rbdClientset:    rbdClientset,
	}
}

// GlobalConfigs global configs
func (cc *GlobalConfigCaseImpl) GlobalConfigs() (*model.GlobalConfigs, error) {
	configs, err := cc.rbdClientset.RainbondV1alpha1().GlobalConfigs(cc.namespace).Get(cc.configName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if configs == nil {
		return nil, fmt.Errorf("do not found cluster config, have you installed rainbond-operator correctly?")
	}
	data, err := cc.k8sModel2Model(configs)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// UpdateGlobalConfig update gloobal config
func (cc *GlobalConfigCaseImpl) UpdateGlobalConfig(data *model.GlobalConfigs) error {
	configs, err := cc.model2K8sModel(data)
	if err != nil {
		return err
	}
	_, err = cc.rbdClientset.RainbondV1alpha1().GlobalConfigs(cc.namespace).Update(configs)
	return err
}

func (cc *GlobalConfigCaseImpl) k8sModel2Model(source *v1alpha1.GlobalConfig) (*model.GlobalConfigs, error) {
	clusterInfo := &model.GlobalConfigs{}
	clusterInfo.ImageHub = &model.ImageHub{
		Domain:    source.Spec.ImageHub.Domain,
		Namespace: source.Spec.ImageHub.Namespace,
		Username:  source.Spec.ImageHub.Username,
		Password:  source.Spec.ImageHub.Password,
	}
	clusterInfo.StorageClassName = source.Spec.StorageClassName
	clusterInfo.RegionDatabase = &model.Database{
		Host:     source.Spec.RegionDatabase.Host,
		Port:     source.Spec.RegionDatabase.Port,
		Username: source.Spec.RegionDatabase.Username,
		Password: source.Spec.RegionDatabase.Password,
	}
	clusterInfo.UIDatabase = &model.Database{
		Host:     source.Spec.UIDatabase.Host,
		Port:     source.Spec.UIDatabase.Port,
		Username: source.Spec.UIDatabase.Username,
		Password: source.Spec.UIDatabase.Password,
	}
	clusterInfo.EtcdConfig = &model.EtcdConfig{
		Endpoints: source.Spec.EtcdConfig.Endpoints,
		UseTLS:    source.Spec.EtcdConfig.UseTLS,
	}
	if source.Spec.EtcdConfig.UseTLS {
		etcdSecret, err := cc.normalClientset.CoreV1().Secrets(cc.namespace).Get(cc.etcdSecretName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		certInfo := &model.EtcdCertInfo{}
		clusterInfo.EtcdConfig.CertInfo = certInfo
		certInfo.CaFile = string(etcdSecret.Data["ca-file"]) // TODO fanyangyang etcd secert data key
		certInfo.CertFile = string(etcdSecret.Data["cert-file"])
		certInfo.KeyFile = string(etcdSecret.Data["key-file"])
	}
	clusterInfo.KubeAPIHost = source.Spec.KubeAPIHost
	for _, portInfo := range source.Spec.NodeAvailPorts {
		clusterInfo.NodeAvailPorts = append(clusterInfo.NodeAvailPorts, &model.NodeAvailPorts{Ports: portInfo.Ports, NodeIP: portInfo.NodeIP, NodeName: portInfo.NodeName})

	}
	return clusterInfo, nil
}

// get old config and then set into new
func (cc *GlobalConfigCaseImpl) model2K8sModel(source *model.GlobalConfigs) (*v1alpha1.GlobalConfig, error) {
	globalConfigSpec := v1alpha1.GlobalConfigSpec{}
	if source.ImageHub != nil {
		globalConfigSpec.ImageHub = v1alpha1.ImageHub{
			Domain:    source.ImageHub.Domain,
			Username:  source.ImageHub.Username,
			Password:  source.ImageHub.Password,
			Namespace: source.ImageHub.Namespace,
		}
	}
	globalConfigSpec.StorageClassName = source.StorageClassName
	if source.RegionDatabase != nil {
		globalConfigSpec.RegionDatabase = v1alpha1.Database{
			Host:     source.RegionDatabase.Host,
			Port:     source.RegionDatabase.Port,
			Username: source.RegionDatabase.Username,
			Password: source.RegionDatabase.Password,
		}
	}
	if source.UIDatabase != nil {
		globalConfigSpec.UIDatabase = v1alpha1.Database{
			Host:     source.UIDatabase.Host,
			Port:     source.UIDatabase.Port,
			Username: source.UIDatabase.Username,
			Password: source.UIDatabase.Password,
		}
	}
	if source.EtcdConfig != nil {
		globalConfigSpec.EtcdConfig = v1alpha1.EtcdConfig{
			Endpoints: source.EtcdConfig.Endpoints,
			UseTLS:    source.EtcdConfig.UseTLS,
		}
		if source.EtcdConfig.UseTLS && source.EtcdConfig.CertInfo != nil {
			if err := cc.updateOrCreateEtcdCertInfo(source.EtcdConfig.CertInfo); err != nil {
				return nil, err
			}
		} else {
			// if update config set etcd that do not use tls, update config, remove etcd cert secret selector
			globalConfigSpec.EtcdConfig.CertSecret = metav1.LabelSelector{}
		}
	}
	globalConfigSpec.KubeAPIHost = source.KubeAPIHost
	if source.NodeAvailPorts != nil {
		for _, port := range source.NodeAvailPorts {
			globalConfigSpec.NodeAvailPorts = append(globalConfigSpec.NodeAvailPorts, v1alpha1.NodeAvailPorts{Ports: port.Ports, NodeIP: port.NodeIP, NodeName: port.NodeName})
		}
	}
	globalConfig := &v1alpha1.GlobalConfig{Spec: globalConfigSpec}
	globalConfig.Name = cc.configName
	globalConfig.Namespace = cc.namespace
	old, err := cc.rbdClientset.RainbondV1alpha1().GlobalConfigs(cc.namespace).Get(cc.configName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	globalConfig.ResourceVersion = old.ResourceVersion
	return globalConfig, nil
}

//TODO generate test case
func (cc *GlobalConfigCaseImpl) updateOrCreateEtcdCertInfo(certInfo *model.EtcdCertInfo) error {
	old, err := cc.normalClientset.CoreV1().Secrets(cc.namespace).Get(cc.etcdSecretName, metav1.GetOptions{})
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			new := &corev1.Secret{}
			new.SetName(cc.etcdSecretName)
			new.SetNamespace(cc.namespace)
			new.Data = make(map[string][]byte)
			new.Data["ca-file"] = []byte(certInfo.CaFile) // TODO fanyangyang etcd cert secret data key
			new.Data["cert-file"] = []byte(certInfo.CertFile)
			new.Data["key-file"] = []byte(certInfo.KeyFile)
			_, err = cc.normalClientset.CoreV1().Secrets(cc.namespace).Create(new)
			return err
		}
		return err
	}
	old.Data["ca-file"] = []byte(certInfo.CaFile) // TODO fanyangyang etcd cert secret data key
	old.Data["cert-file"] = []byte(certInfo.CertFile)
	old.Data["key-file"] = []byte(certInfo.KeyFile)
	_, err = cc.normalClientset.CoreV1().Secrets(cc.namespace).Update(old)
	return err
}

/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/distribution/reference"
	dtypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	dclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/go-logr/logr"
	"github.com/goodrain/rainbond-operator/util/commonutil"
	"github.com/goodrain/rainbond-operator/util/constants"
	"github.com/goodrain/rainbond-operator/util/downloadutil"
	"github.com/goodrain/rainbond-operator/util/rbdutil"
	"github.com/goodrain/rainbond-operator/util/retryutil"
	"github.com/goodrain/rainbond-operator/util/tarutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rainbondv1alpha1 "github.com/goodrain/rainbond-operator/api/v1alpha1"
)

var errorClusterConfigNotReady = fmt.Errorf("cluster config can not be ready")
var errorClusterConfigNoLocalHub = fmt.Errorf("cluster spec not have local image hub info ")
var pkgDst = "/opt/rainbond/pkg/files"

// RainbondPackageReconciler reconciles a RainbondPackage object
type RainbondPackageReconciler struct {
	client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=rainbond.io,resources=rainbondpackages,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rainbond.io,resources=rainbondpackages/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rainbond.io,resources=rainbondpackages/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the RainbondPackage object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
func (r *RainbondPackageReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("rainbondpackage", request.NamespacedName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fetch the RainbondPackage instance
	pkg := &rainbondv1alpha1.RainbondPackage{}
	err := r.Get(ctx, request.NamespacedName, pkg)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	cluster := &rainbondv1alpha1.RainbondCluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: pkg.Namespace, Name: constants.RainbondClusterName}, cluster); err != nil {
		log.Error(err, "get rainbondcluster.")
		return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
	}

	if !cluster.Spec.ConfigCompleted {
		log.V(6).Info("rainbondcluster is not completed, waiting!!")
		return reconcile.Result{RequeueAfter: 3 * time.Second}, nil
	}

	// if instsall mode is full online, set package to ready directly
	if cluster.Spec.InstallMode == rainbondv1alpha1.InstallationModeFullOnline {
		log.Info("set package to ready directly", "install mode", cluster.Spec.InstallMode)
		pkg.Status = initPackageStatus(rainbondv1alpha1.Completed)
		if err := updateCRStatus(r.Client, pkg); err != nil {
			log.Error(err, "update package status")
			return reconcile.Result{RequeueAfter: time.Second * 5}, nil
		}
		return reconcile.Result{}, nil
	}

	updateStatus, re := checkStatusCanReturn(pkg)
	if updateStatus {
		if err := updateCRStatus(r.Client, pkg); err != nil {
			log.Error(err, "update package status failure ")
			return reconcile.Result{RequeueAfter: time.Second * 5}, nil
		}
		return reconcile.Result{}, nil
	}
	if re != nil {
		return *re, nil
	}

	//need handle condition
	p, err := newpkg(ctx, r.Client, pkg, cluster, log)
	if err != nil {
		if p != nil {
			p.updateConditionStatus(rainbondv1alpha1.Init, rainbondv1alpha1.Failed)
			p.updateConditionResion(rainbondv1alpha1.Init, err.Error(), "create package handle failure")
			p.updateCRStatus()
		}
		log.Error(err, "create package handle failure ")
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}

	// handle package
	if err = p.handle(); err != nil {
		if err == errorClusterConfigNoLocalHub {
			log.V(4).Info("waiting local image hub ready")
		} else if err == errorClusterConfigNotReady {
			log.Info("waiting cluster config ready")
		} else {
			log.Error(err, "failed to handle rainbond package.")
		}
		return reconcile.Result{RequeueAfter: 8 * time.Second}, nil
	}

	return reconcile.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RainbondPackageReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rainbondv1alpha1.RainbondPackage{}).
		Complete(r)
}

func initPackageStatus(status rainbondv1alpha1.PackageConditionStatus) rainbondv1alpha1.RainbondPackageStatus {
	return rainbondv1alpha1.RainbondPackageStatus{
		Conditions: []rainbondv1alpha1.PackageCondition{
			{
				Type:               rainbondv1alpha1.Init,
				Status:             status,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
			},
			{
				Type:               rainbondv1alpha1.DownloadPackage,
				Status:             status,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
			},
			{
				Type:               rainbondv1alpha1.UnpackPackage,
				Status:             status,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
			},
			{
				Type:               rainbondv1alpha1.PushImage,
				Status:             status,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
			},
			{
				Type:               rainbondv1alpha1.Ready,
				Status:             status,
				LastHeartbeatTime:  metav1.Now(),
				LastTransitionTime: metav1.Now(),
			},
		},
		ImagesPushed: []rainbondv1alpha1.RainbondPackageImage{},
	}
}

//checkStatusCanReturn if pkg status in the working state, straight back
func checkStatusCanReturn(pkg *rainbondv1alpha1.RainbondPackage) (updateStatus bool, re *reconcile.Result) {
	if len(pkg.Status.Conditions) == 0 {
		pkg.Status = initPackageStatus(rainbondv1alpha1.Waiting)
		return true, &reconcile.Result{}
	}
	completedCount := 0
	for _, cond := range pkg.Status.Conditions {
		if cond.Status == rainbondv1alpha1.Running {
			return false, &reconcile.Result{}
		}
		//have failed conditions, retry
		if cond.Status == rainbondv1alpha1.Failed {
			return false, nil
		}

		if cond.Status == rainbondv1alpha1.Completed {
			completedCount++
		}
	}
	if completedCount == len(pkg.Status.Conditions) {
		return false, &reconcile.Result{}
	}
	return false, nil
}

type pkg struct {
	ctx              context.Context
	client           client.Client
	dcli             *dclient.Client
	pkg              *rainbondv1alpha1.RainbondPackage
	cluster          *rainbondv1alpha1.RainbondCluster
	log              logr.Logger
	downloadPackage  bool
	localPackagePath string
	// Deprecated: no longer download installation package.
	downloadPackageURL string
	// Deprecated: no longer download installation package.
	downloadPackageMD5  string
	downloadImageDomain string
	pushImageDomain     string
	// Deprecated: no longer download installation package.
	totalImageNum int32
	//need download images
	images  map[string]string
	version string
}

func newpkg(ctx context.Context, client client.Client, p *rainbondv1alpha1.RainbondPackage, cluster *rainbondv1alpha1.RainbondCluster, reqLogger logr.Logger) (*pkg, error) {
	dcli, err := newDockerClient(ctx)
	if err != nil {
		reqLogger.Error(err, "failed to create docker client")
		return nil, err
	}
	pkg := &pkg{
		ctx:    ctx,
		client: client,
		pkg:    p.DeepCopy(),
		dcli:   dcli,
		// Deprecated: no longer download installation package.
		totalImageNum: 23,
		images:        make(map[string]string, 23),
		log:           reqLogger,
		version:       cluster.Spec.InstallVersion,
		cluster:       cluster,
	}
	return pkg, nil
}

func (p *pkg) configByCluster(c *rainbondv1alpha1.RainbondCluster) error {
	if !c.Spec.ConfigCompleted {
		return errorClusterConfigNotReady
	}

	// check if image repository is ready
	if !p.isImageRepositoryReady() {
		return errorClusterConfigNoLocalHub
	}

	if c.Spec.InstallVersion != "" {
		p.version = c.Spec.InstallVersion
	}
	p.localPackagePath = p.pkg.Spec.PkgPath
	ciVersion := c.Spec.CIVersion
	if ciVersion == "" {
		ciVersion = "v5.3.3"
	}
	p.images = map[string]string{
		"/builder:" + ciVersion:             "/builder",
		"/runner:" + ciVersion:              "/runner",
		"/rbd-init-probe:" + p.version:      "/rbd-init-probe",
		"/rbd-mesh-data-panel:" + p.version: "/rbd-mesh-data-panel",
		"/plugins-tcm:5.1.7":                "/tcm",
	}
	return nil
}

func (p *pkg) updateCRStatus() error {
	return updateCRStatus(p.client, p.pkg)
}

func updateCRStatus(client client.Client, pkg *rainbondv1alpha1.RainbondPackage) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &rainbondv1alpha1.RainbondPackage{}
		if err := client.Get(ctx, types.NamespacedName{Namespace: pkg.Namespace, Name: pkg.Name}, latest); err != nil {
			return fmt.Errorf("getting latest rainbond package: %v", err)
		}
		pkg.ResourceVersion = latest.ResourceVersion
		return client.Status().Update(ctx, pkg)
	}); err != nil {
		return fmt.Errorf("failed to update rainbondpackage status: %v", err)
	}
	return nil
}

func (p *pkg) checkClusterConfig() error {
	cluster := p.cluster
	if err := p.configByCluster(cluster); err != nil {
		return err
	}
	switch cluster.Spec.InstallMode {
	default:
		p.downloadImageDomain = cluster.Spec.RainbondImageRepository
		if p.downloadImageDomain == "" {
			p.downloadImageDomain = "rainbond"
		}
		if cluster.Spec.ImageHub != nil {
			p.pushImageDomain = cluster.Spec.ImageHub.Domain
			if cluster.Spec.ImageHub.Namespace != "" {
				p.pushImageDomain += "/" + cluster.Spec.ImageHub.Namespace
			}
		}
		if p.pushImageDomain == "" {
			p.pushImageDomain = constants.DefImageRepository
		}
	}
	return nil
}

func (p *pkg) findCondition(typ3 rainbondv1alpha1.PackageConditionType) *rainbondv1alpha1.PackageCondition {
	for i, condition := range p.pkg.Status.Conditions {
		if condition.Type == typ3 {
			return &p.pkg.Status.Conditions[i]
		}
	}
	return nil
}
func (p *pkg) updateConditionStatus(typ3 rainbondv1alpha1.PackageConditionType, status rainbondv1alpha1.PackageConditionStatus) {
	for i, condition := range p.pkg.Status.Conditions {
		if condition.Type == typ3 {
			if p.pkg.Status.Conditions[i].Status != status {
				p.pkg.Status.Conditions[i].LastTransitionTime = metav1.Now()
			}
			p.pkg.Status.Conditions[i].LastHeartbeatTime = metav1.Now()
			p.pkg.Status.Conditions[i].Status = status
			if status == rainbondv1alpha1.Completed {
				p.pkg.Status.Conditions[i].Progress = 100
				p.pkg.Status.Conditions[i].Reason = ""
				p.pkg.Status.Conditions[i].Message = ""
			}
			break
		}
	}
}
func (p *pkg) updateConditionResion(typ3 rainbondv1alpha1.PackageConditionType, resion, message string) {
	for i, condition := range p.pkg.Status.Conditions {
		if condition.Type == typ3 {
			p.pkg.Status.Conditions[i].LastHeartbeatTime = metav1.Now()
			p.pkg.Status.Conditions[i].Reason = resion
			p.pkg.Status.Conditions[i].Message = message
			break
		}
	}
}
func (p *pkg) updateConditionProgress(typ3 rainbondv1alpha1.PackageConditionType, progress int32) bool {
	if progress > 100 {
		progress = 100
	}
	for i, condition := range p.pkg.Status.Conditions {
		if condition.Type == typ3 {
			p.pkg.Status.Conditions[i].LastHeartbeatTime = metav1.Now()
			if p.pkg.Status.Conditions[i].Progress != int(progress) {
				p.pkg.Status.Conditions[i].Progress = int(progress)
				return true
			}
		}
	}
	return false
}
func (p *pkg) completeCondition(con *rainbondv1alpha1.PackageCondition) error {
	if con == nil {
		return nil
	}
	p.updateConditionStatus(con.Type, rainbondv1alpha1.Completed)
	return p.updateCRStatus()
}
func (p *pkg) runningCondition(con *rainbondv1alpha1.PackageCondition) error {
	if con == nil {
		return nil
	}
	p.updateConditionStatus(con.Type, rainbondv1alpha1.Running)
	return p.updateCRStatus()
}
func (p *pkg) canDownload() bool {
	if con := p.findCondition(rainbondv1alpha1.DownloadPackage); con != nil {
		if con.Status != rainbondv1alpha1.Completed {
			if !p.downloadPackage {
				p.log.Info("not need download package")
				if err := p.completeCondition(con); err != nil {
					p.log.Error(err, "complete download condition because of not need download failure %s")
				}
				return false
			}
			if err := p.runningCondition(con); err != nil {
				p.log.Error(err, "complete download condition because of not need download failure %s")
			}
			return true
		}
		return false
	}
	return false
}

func (p *pkg) canUnpack() bool {
	if con := p.findCondition(rainbondv1alpha1.DownloadPackage); con != nil {
		//Must conditions are not met
		if con.Status != rainbondv1alpha1.Completed {
			return false
		}
		if uncon := p.findCondition(rainbondv1alpha1.UnpackPackage); uncon != nil {
			//status is waiting
			if uncon.Status != rainbondv1alpha1.Completed {
				if !p.downloadPackage {
					if err := p.completeCondition(uncon); err != nil {
						p.log.Error(err, "complete unpack package condition because of not need download failure %s")
					}
					return false
				}
				if err := p.runningCondition(uncon); err != nil {
					p.log.Error(err, "running unpack package condition failure %s")
				}
				return true
			}
		}
	}
	return false
}
func (p *pkg) canPushImage() bool {
	if uncon := p.findCondition(rainbondv1alpha1.UnpackPackage); uncon != nil {
		if uncon.Status != rainbondv1alpha1.Completed {
			return false
		}
		if pcon := p.findCondition(rainbondv1alpha1.PushImage); pcon != nil {
			if pcon.Status != rainbondv1alpha1.Completed {
				return true
			}
		}
	}
	return false
}

func (p *pkg) canReady() bool {
	if pcon := p.findCondition(rainbondv1alpha1.PushImage); pcon != nil && pcon.Status == rainbondv1alpha1.Completed {
		return true
	}
	return false
}
func (p *pkg) setInitStatus() error {
	if con := p.findCondition(rainbondv1alpha1.Init); con != nil {
		if con.Status != rainbondv1alpha1.Completed {
			con.Status = rainbondv1alpha1.Completed
			if err := p.updateCRStatus(); err != nil {
				p.log.Error(err, "failed to update rainbondpackage status.")
				return err
			}
		}
	}
	return nil
}

//donwnloadPackage download package
func (p *pkg) donwnloadPackage() error {
	p.log.Info(fmt.Sprintf("start download package from %s", p.downloadPackageURL))
	downloadListener := &downloadutil.DownloadWithProgress{
		URL:       p.downloadPackageURL,
		SavedPath: p.localPackagePath,
		Wanted:    p.downloadPackageMD5,
	}
	// first chack exist file md5
	file, _ := os.Open(p.localPackagePath)
	if file != nil {
		err := downloadListener.CheckMD5(file)
		_ = file.Close()
		if err == nil {
			p.log.Info("rainbond package file is exists")
			return nil
		}
	}
	p.log.Info("rainbond package file does not exists, downloading background ...")
	var stop = make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(time.Second * 3)
		for {
			select {
			case <-ticker.C:
				progress := downloadListener.Percent
				//Make time for later in the download process
				realProgress := int32(progress) - int32(float64(progress)*0.05)
				if p.updateConditionProgress(rainbondv1alpha1.DownloadPackage, realProgress) {
					if err := p.updateCRStatus(); err != nil {
						// ignore error
						p.log.Info("update number extracted: %v", err)
					}
				}
			case <-stop:
				return
			}
		}
	}()
	if err := downloadListener.Download(); err != nil {
		p.log.Error(err, "download rainbond package error, will retry")
		p.updateConditionResion(rainbondv1alpha1.Init, err.Error(), "download rainbond package error, will retry")
		p.updateCRStatus()
		err = downloadListener.Download()
		if err != nil {
			p.log.Error(err, "download rainbond package error, not retry")
			return err
		}
	}
	//stop watch progress
	stop <- struct{}{}
	p.log.Info(fmt.Sprintf("success download package from %s", p.downloadPackageURL))
	return nil
}

//handle
func (p *pkg) handle() error {
	p.log.V(5).Info("start handling rainbond package.")
	// check prerequisites
	if err := p.checkClusterConfig(); err != nil {
		p.log.V(6).Info(fmt.Sprintf("check cluster config: %v", err))
		//To continue waiting
		if err == errorClusterConfigNotReady || err == errorClusterConfigNoLocalHub {
			return err
		}
		p.updateConditionStatus(rainbondv1alpha1.Init, rainbondv1alpha1.Waiting)
		p.updateConditionResion(rainbondv1alpha1.Init, err.Error(), "get rainbond cluster config failure")
		p.updateCRStatus()
		return err
	}
	//update init condition status is complete
	if err := p.setInitStatus(); err != nil {
		p.log.Error(err, "set init status")
		p.updateConditionStatus(rainbondv1alpha1.Init, rainbondv1alpha1.Failed)
		p.updateConditionResion(rainbondv1alpha1.Init, err.Error(), "set init status failure")
		p.updateCRStatus()
		return err
	}
	if p.canDownload() {
		//download pkg
		if err := p.donwnloadPackage(); err != nil {
			p.log.Error(err, "download package")
			p.updateConditionStatus(rainbondv1alpha1.DownloadPackage, rainbondv1alpha1.Failed)
			p.updateConditionResion(rainbondv1alpha1.DownloadPackage, err.Error(), "download package failure")
			p.updateCRStatus()
			return fmt.Errorf("failed to download package %s", err.Error())
		}
		p.log.Info("handle downlaod package success")
		p.updateConditionStatus(rainbondv1alpha1.DownloadPackage, rainbondv1alpha1.Completed)
		return p.updateCRStatus()
	}

	if p.canUnpack() {
		//unstar the installation package
		if err := p.untartar(); err != nil {
			p.updateConditionStatus(rainbondv1alpha1.UnpackPackage, rainbondv1alpha1.Failed)
			p.updateConditionResion(rainbondv1alpha1.UnpackPackage, err.Error(), "unpack package failure")
			p.updateCRStatus()
			return fmt.Errorf("failed to untar %s: %v", p.pkg.Spec.PkgPath, err)
		}
		p.log.Info("handle package unpack success")
		p.updateConditionStatus(rainbondv1alpha1.UnpackPackage, rainbondv1alpha1.Completed)
		return p.updateCRStatus()
	}

	if p.canPushImage() {
		// Deprecated: No longer download the installation package
		if p.downloadPackage {
			p.log.Info("start load and push images")
			if err := p.imagesLoadAndPush(); err != nil {
				p.updateConditionStatus(rainbondv1alpha1.PushImage, rainbondv1alpha1.Failed)
				p.updateConditionResion(rainbondv1alpha1.PushImage, err.Error(), "load and push images failure")
				p.updateCRStatus()
				return fmt.Errorf("failed to load and push images: %v", err)
			}
		} else {
			p.log.Info("start pull and push images")
			if err := p.imagePullAndPush(); err != nil {
				p.updateConditionStatus(rainbondv1alpha1.PushImage, rainbondv1alpha1.Failed)
				p.updateConditionResion(rainbondv1alpha1.PushImage, err.Error(), "pull and push images failure")
				p.updateCRStatus()
				return fmt.Errorf("failed to pull and push images: %v", err)
			}
		}
		p.log.Info("handle images success")
		p.updateConditionStatus(rainbondv1alpha1.PushImage, rainbondv1alpha1.Completed)
		return p.updateCRStatus()
	}

	if p.canReady() {
		p.updateConditionStatus(rainbondv1alpha1.Ready, rainbondv1alpha1.Completed)
		return p.updateCRStatus()
	}
	p.log.V(5).Info("no event can be handle about package")
	return nil
}

func (p *pkg) untartar() error {
	p.log.Info(fmt.Sprintf("start untartaring %s", p.pkg.Spec.PkgPath))
	f, err := os.Open(p.pkg.Spec.PkgPath)
	if f != nil {
		f.Close()
	}
	if err != nil {
		return err
	}
	stop := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(time.Second * 2)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				num := countImages(pkgDst)
				progress := num * 100 / p.totalImageNum
				if p.updateConditionProgress(rainbondv1alpha1.UnpackPackage, progress) {
					if err := p.updateCRStatus(); err != nil {
						// ignore error
						p.log.Info(fmt.Sprintf("update number extracted: %v", err))
					}
				}
			case <-stop:
				return
			}
		}
	}()
	_ = os.MkdirAll(pkgDst, os.ModePerm)
	if err := tarutil.Untartar(p.pkg.Spec.PkgPath, pkgDst); err != nil {
		return err
	}
	stop <- struct{}{}
	return nil
}
func (p *pkg) imagePullAndPush() error {
	p.pkg.Status.ImagesNumber = int32(len(p.images))
	p.pkg.Status.ImagesPushed = nil
	var count int32
	handleImgae := func(remoteImage, localImage string) error {
		return retryutil.Retry(time.Second*2, 3, func() (bool, error) {
			exists, err := p.checkIfImageExists(remoteImage)
			if err != nil {
				return false, fmt.Errorf("check if image exists: %v", err)
			}
			if !exists {
				p.log.V(4).Info("image does not exists, start pulling", "image name", remoteImage)
				if err := p.imagePull(remoteImage); err != nil {
					return false, fmt.Errorf("pull image %s failure %s", remoteImage, err.Error())
				}
			}
			if err := p.dcli.ImageTag(p.ctx, remoteImage, localImage); err != nil {
				return false, fmt.Errorf("change image tag(%s => %s) failure: %v", remoteImage, localImage, err)
			}
			if err := p.imagePush(localImage); err != nil {
				return false, fmt.Errorf("push image %s failure %s", localImage, err.Error())
			}
			return true, nil
		})
	}

	for old, new := range p.images {
		remoteImage := path.Join(p.downloadImageDomain, old)
		localImage := path.Join(p.pushImageDomain, new)
		if err := handleImgae(remoteImage, localImage); err != nil {
			return err
		}
		count++
		p.pkg.Status.ImagesPushed = append(p.pkg.Status.ImagesPushed, rainbondv1alpha1.RainbondPackageImage{Name: localImage})
		progress := count * 100 / p.pkg.Status.ImagesNumber
		if p.updateConditionProgress(rainbondv1alpha1.PushImage, progress) {
			if err := p.updateCRStatus(); err != nil {
				return fmt.Errorf("update cr status: %v", err)
			}
		}
		p.log.Info("successfully load image", "image", localImage)
	}
	return nil
}
func (p *pkg) imagesLoadAndPush() error {
	p.pkg.Status.ImagesNumber = countImages(pkgDst)
	p.pkg.Status.ImagesPushed = nil
	var count int32
	walkFn := func(pstr string, info os.FileInfo, err error) error {
		l := p.log.WithValues("file", pstr)
		if err != nil {
			l.Info(fmt.Sprintf("prevent panic by handling failure accessing a path %q: %v\n", pstr, err))
			return fmt.Errorf("prevent panic by handling failure accessing a path %q: %v", pstr, err)
		}
		if !commonutil.IsFile(pstr) {
			return nil
		}
		if !validateFile(pstr) {
			l.Info("invalid file, skip it1")
			return nil
		}

		f := func() (bool, error) {
			image, err := p.imageLoad(pstr)
			if err != nil {
				l.Error(err, "load image")
				return false, fmt.Errorf("load image: %v", err)
			}

			newImage := newImageWithNewDomain(image, rbdutil.GetImageRepository(p.cluster))
			if newImage == "" {
				return false, fmt.Errorf("parse image name failure")
			}

			if err := p.dcli.ImageTag(p.ctx, image, newImage); err != nil {
				l.Error(err, "tag image", "source", image, "target", newImage)
				return false, fmt.Errorf("tag image: %v", err)
			}

			if err = p.imagePush(newImage); err != nil {
				l.Error(err, "push image", "image", newImage)
				return false, fmt.Errorf("push image %s: %v", newImage, err)
			}
			count++
			p.pkg.Status.ImagesPushed = append(p.pkg.Status.ImagesPushed, rainbondv1alpha1.RainbondPackageImage{Name: newImage})
			progress := count * 100 / p.pkg.Status.ImagesNumber
			if p.updateConditionProgress(rainbondv1alpha1.PushImage, progress) {
				if err := p.updateCRStatus(); err != nil {
					return false, fmt.Errorf("update cr status: %v", err)
				}
			}
			l.Info("successfully load image", "image", newImage)
			return true, nil
		}

		return retryutil.Retry(1*time.Second, 3, f)
	}

	return filepath.Walk(pkgDst, walkFn)
}

func (p *pkg) imageLoad(file string) (string, error) {
	p.log.Info("start loading image", "file", file)
	f, err := os.Open(file)
	if err != nil {
		return "", fmt.Errorf("open file %s: %v", file, err)
	}
	defer f.Close()
	res, err := p.dcli.ImageLoad(p.ctx, f, true) // load one, push one.
	if err != nil {
		return "", fmt.Errorf("path: %s; failed to load images: %v", file, err)
	}
	var imageName string
	if res.Body != nil {
		defer res.Body.Close()
		dec := json.NewDecoder(res.Body)
		for {
			select {
			case <-p.ctx.Done():
				p.log.Error(p.ctx.Err(), "error form context")
				return "", p.ctx.Err()
			default:
			}
			var jm jsonmessage.JSONMessage
			if err := dec.Decode(&jm); err != nil {
				if err == io.EOF {
					break
				}
				return "", fmt.Errorf("failed to decode json message: %v", err)
			}
			if jm.Error != nil {
				return "", fmt.Errorf("error detail: %v", jm.Error)
			}
			msg := jm.Stream
			//the domain of image in package is goodrain.me,not need change tag
			image := parseImageName(msg)
			if image != "" {
				imageName = image
			}
		}
	}
	if imageName == "" {
		return "", fmt.Errorf("not parse image name")
	}
	p.log.Info("success loading image", "image", imageName)
	return imageName, nil
}

func (p *pkg) imagePush(image string) error {
	p.log.Info("start push image", "image", image)
	var pullipo dtypes.ImagePushOptions
	if p.cluster != nil && p.cluster.Spec.ImageHub != nil && p.cluster.Spec.ImageHub.Username != "" {
		auth, err := EncodeAuthToBase64(dtypes.AuthConfig{
			Username: p.cluster.Spec.ImageHub.Username,
			Password: p.cluster.Spec.ImageHub.Password,
		})
		if err != nil {
			p.log.Error(err, "Encode image hub user and password failure")
			return err
		}
		pullipo = dtypes.ImagePushOptions{
			RegistryAuth: auth,
		}
	} else {
		pullipo = dtypes.ImagePushOptions{}
	}
	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()
	var res io.ReadCloser
	res, err := p.dcli.ImagePush(ctx, image, pullipo)
	if err != nil {
		p.log.Error(err, "failed to push image", "image", image)
		return err
	}
	if res != nil {
		defer res.Close()

		dec := json.NewDecoder(res)
		for {
			select {
			case <-ctx.Done():
				p.log.Error(p.ctx.Err(), "error form context")
				return p.ctx.Err()
			default:
			}
			var jm jsonmessage.JSONMessage
			if err := dec.Decode(&jm); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("failed to decode json message: %v", err)
			}
			if jm.Error != nil {
				return fmt.Errorf("error detail: %v", jm.Error)
			}
		}
	}
	p.log.Info("success push image", "image", image)
	return nil
}

func (p *pkg) imagePull(image string) error {
	p.log.Info("start pull image", "image", image)
	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()
	rf, err := reference.ParseAnyReference(image)
	if err != nil {
		p.log.Error(err, "reference image error")
		return err
	}
	var pullipo dtypes.ImagePullOptions
	if p.pkg.Spec.ImageHubUser != "" {
		auth, err := EncodeAuthToBase64(dtypes.AuthConfig{Username: p.pkg.Spec.ImageHubUser, Password: p.pkg.Spec.ImageHubPass})
		if err != nil {
			p.log.Error(err, "Encode image hub user and password failure")
			return err
		}
		pullipo = dtypes.ImagePullOptions{
			RegistryAuth: auth,
		}
	} else {
		pullipo = dtypes.ImagePullOptions{}
	}
	res, err := p.dcli.ImagePull(ctx, rf.String(), pullipo)
	if err != nil {
		return fmt.Errorf("pull image %s failure %s", image, err.Error())
	}
	if res != nil {
		defer res.Close()
		dec := json.NewDecoder(res)
		for {
			select {
			case <-ctx.Done():
				p.log.Error(ctx.Err(), "error form context")
				return ctx.Err()
			default:
			}
			var jm jsonmessage.JSONMessage
			if err := dec.Decode(&jm); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("failed to decode json message: %v", err)
			}
			if jm.Error != nil {
				return fmt.Errorf("error detail: %v", jm.Error)
			}
		}
	}
	p.log.Info("success pull image", "image", image)
	return nil
}

// EncodeAuthToBase64 serializes the auth configuration as JSON base64 payload
func EncodeAuthToBase64(authConfig dtypes.AuthConfig) (string, error) {
	buf, err := json.Marshal(authConfig)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf), nil
}

func newDockerClient(ctx context.Context) (*dclient.Client, error) {
	cli, err := dclient.NewClientWithOpts(dclient.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("create new docker client: %v", err)
	}
	cli.NegotiateAPIVersion(ctx)

	return cli, nil
}

func parseImageName(str string) string {
	if !strings.Contains(str, "Loaded image: ") {
		return ""
	}
	str = strings.Replace(str, "Loaded image: ", "", -1)
	str = strings.Replace(str, "\n", "", -1)
	str = trimLatest(str)
	return str
}

func trimLatest(str string) string {
	if !strings.HasSuffix(str, ":latest") {
		return str
	}
	return str[:len(str)-len(":latest")]
}

func countImages(dir string) int32 {
	var count int32
	_ = filepath.Walk(dir, func(pstr string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !commonutil.IsFile(pstr) {
			return nil
		}
		if !validateFile(pstr) {
			return nil
		}
		count++
		return nil
	})

	return count
}

func validateFile(file string) bool {
	base := path.Base(file)
	if path.Ext(base) != ".tgz" || strings.HasPrefix(base, "._") {
		return false
	}
	return true
}

func newImageWithNewDomain(image string, newDomain string) string {
	repo, err := reference.Parse(image)
	if err != nil {
		return ""
	}
	named := repo.(reference.Named)
	remoteName := reference.Path(named)
	tag := "latest"
	if t, ok := repo.(reference.Tagged); ok {
		tag = t.Tag()
	}
	return path.Join(newDomain, remoteName+":"+tag)
}

func (p *pkg) checkIfImageExists(image string) (bool, error) {
	repo, err := reference.Parse(image)
	if err != nil {
		p.log.V(6).Info("parse image", "image", image, "error", err)
		return false, fmt.Errorf("parse image %s: %v", image, err)
	}
	named := repo.(reference.Named)
	tag := "latest"
	if t, ok := repo.(reference.Tagged); ok {
		tag = t.Tag()
	}
	imageFullName := named.Name() + ":" + tag

	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	imageSummarys, err := p.dcli.ImageList(ctx, dtypes.ImageListOptions{
		Filters: filters.NewArgs(filters.KeyValuePair{Key: "reference", Value: imageFullName}),
	})
	if err != nil {
		return false, fmt.Errorf("list images: %v", err)
	}
	for _, imageSummary := range imageSummarys {
		fmt.Printf("%#v", imageSummary.RepoTags)
	}

	_ = imageSummarys

	return len(imageSummarys) > 0, nil
}

func (p *pkg) isImageRepositoryReady() bool {

	idx, condition := p.cluster.Status.GetCondition(rainbondv1alpha1.RainbondClusterConditionTypeImageRepository)
	if idx == -1 {
		return false
	}

	if condition.Status != corev1.ConditionTrue {
		return false
	}

	return true
}

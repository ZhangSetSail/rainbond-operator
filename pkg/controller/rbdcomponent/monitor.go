package rbdcomponent

import (
	rainbondv1alpha1 "github.com/GLYASAI/rainbond-operator/pkg/apis/rainbond/v1alpha1"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var monitorName = "rbd-monitor"

func resourcesForMonitor(r *rainbondv1alpha1.RbdComponent) []interface{} {
	return []interface{}{
		daemonSetForMonitor(r),
	}
}

func daemonSetForMonitor(r *rainbondv1alpha1.RbdComponent) interface{} {
	labels := r.Labels()
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      monitorName,
			Namespace: r.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   monitorName,
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Tolerations: []corev1.Toleration{
						{
							Key:    "node-role.kubernetes.io/master",
							Effect: corev1.TaintEffectNoSchedule,
						},
					},
					NodeSelector: map[string]string{
						"node-role.kubernetes.io/master": "",
					},
					Containers: []corev1.Container{
						{
							Name:            monitorName,
							Image:           r.Spec.Image,
							ImagePullPolicy: corev1.PullIfNotPresent, // TODO: custom
							Env: []corev1.EnvVar{
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
							},
							Args: []string{
								"--etcd-endpoints=http://etcd0:2379",
								"--advertise-addr=$(POD_IP):9999",
								"--alertmanager-address=$(POD_IP):9093",
								"--storage.tsdb.path=/prometheusdata",
								"--storage.tsdb.no-lockfile",
								"--storage.tsdb.retention=7d",
								"--log.level=debug",
							},
						},
					},
				},
			},
		},
	}

	return ds
}

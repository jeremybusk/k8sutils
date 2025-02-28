package main

import (
        "bufio"
        "context"
        "crypto/rand"
        "io"
        // "encoding/base64"
        "flag"
        "fmt"
        "log"
        "math/big"
        "os"
        "os/exec"
        "strings"
        "time"

        corev1 "k8s.io/api/core/v1"
        "k8s.io/apimachinery/pkg/api/resource"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/apimachinery/pkg/types"
        "k8s.io/client-go/kubernetes"
        "k8s.io/client-go/tools/clientcmd"
)

var (
        bwLimit         = flag.Int("bwlimit", 10240, "Bandwidth limit for rsync in KB/s")
        oldPVC          = flag.String("old-pvc", "", "Old PVC name")
        newSizeGi       = flag.Int("size", 0, "New PVC size in GiB")
        newStorageClass = flag.String("storage-class", "", "New storage class")
        namespace       = flag.String("namespace", "", "Kubernetes namespace")
        sshPort         = flag.Int("ssh-port", 19022, "SSH server port")
)

func main() {
        flag.Parse()

        if *oldPVC == "" || *newSizeGi <= 0 || *newStorageClass == "" || *namespace == "" {
                fmt.Println("Usage: --old-pvc <name> --size <Gi> --storage-class <class> --namespace <namespace>")
                os.Exit(1)
        }

        tmpPVC := *oldPVC + "-tmp"
        srcPod := "src-pod"
        dstPod := "dst-pod"
        username := "root"

        config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
        if err != nil {
                log.Fatalf("Error building kubeconfig: %v", err)
        }

        clientset, err := kubernetes.NewForConfig(config)
        if err != nil {
                log.Fatalf("Error creating Kubernetes client: %v", err)
        }

        podList, err := clientset.CoreV1().Pods(*namespace).List(context.TODO(), metav1.ListOptions{})
        if err != nil {
                log.Fatalf("Error getting pods: %v", err)
        }

        var podName, ownerKind, ownerName string
        for _, pod := range podList.Items {
                for _, vol := range pod.Spec.Volumes {
                        if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == *oldPVC {
                                podName = pod.Name
                                if len(pod.OwnerReferences) > 0 {
                                        ownerKind = pod.OwnerReferences[0].Kind
                                        ownerName = pod.OwnerReferences[0].Name
                                }
                                break
                        }
                }
        }

        if podName == "" {
                log.Printf("No running pod found using PVC: %s", oldPVC)
        }

        scaleWorkload(clientset, ownerKind, ownerName, *namespace, 0)

        password := randomString(12)

        createPVC(clientset, tmpPVC, int64(*newSizeGi), *newStorageClass, *namespace)

        startSourcePod(*oldPVC, *namespace, srcPod, *sshPort, "root", password)
        waitForPodRunning(srcPod, *namespace)

        startDestPod(tmpPVC, *namespace, dstPod, srcPod, *bwLimit, *sshPort, "root", password)
        waitForPodSuccess(dstPod, *namespace)

        // cleanup
        log.Printf("Sleep 20 before round 1 clean-up.")
        time.Sleep(20 * time.Second)
        getPodLogs(clientset, dstPod, *namespace)
        exec.Command("kubectl", "delete", "pod", dstPod, "-n", *namespace).Run()
        log.Printf("Deleted data copy pod %s", dstPod)
        getPodLogs(clientset, srcPod, *namespace)
        exec.Command("kubectl", "delete", "pod", srcPod, "-n", *namespace).Run()
        log.Printf("Deleted data copy pod %s", srcPod)
        deletePVC(clientset, *oldPVC, *namespace)
        createPVC(clientset, *oldPVC, int64(*newSizeGi), *newStorageClass, *namespace)

        time.Sleep(20 * time.Second)
        startSourcePod(tmpPVC, *namespace, srcPod, *sshPort, username, password)
        waitForPodRunning(srcPod, *namespace)

        startDestPod(*oldPVC, *namespace, dstPod, srcPod, *bwLimit, *sshPort, username, password)
        waitForPodSuccess(dstPod, *namespace)

        // cleanup
        log.Printf("Sleep 20 before round 2 clean-up.")
        time.Sleep(20 * time.Second)
        getPodLogs(clientset, dstPod, *namespace)
        exec.Command("kubectl", "delete", "pod", dstPod, "-n", *namespace).Run()
        log.Printf("Deleted data copy pod %s", dstPod)
        getPodLogs(clientset, srcPod, *namespace)
        exec.Command("kubectl", "delete", "pod", srcPod, "-n", *namespace).Run()
        log.Printf("Deleted data copy pod %s", srcPod)
        deletePVC(clientset, tmpPVC, *namespace)
        scaleWorkload(clientset, ownerKind, ownerName, *namespace, 1)

        log.Println("PVC migration completed successfully.")
}

// Follow: true, // Follow the logs as they are produced
// Previous:  true,
func printPodLogs(clientset *kubernetes.Clientset, podName string, namespace string) error {
        podLogOptions := corev1.PodLogOptions{
                Follow: true,
        }

        req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &podLogOptions)
        podLogs, err := req.Stream(context.TODO())
        if err != nil {
                return fmt.Errorf("error in opening stream: %v", err)
        }
        defer podLogs.Close()

        reader := bufio.NewReader(podLogs)
        for {
                line, err := reader.ReadString('\n')
                if err == io.EOF {
                        break
                } else if err != nil {
                        return fmt.Errorf("error in reading stream: %v", err)
                }
                fmt.Print(line)
        }
        return nil

}

func randomString(n int) string {
        const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        result := make([]byte, n)
        for i := range result {
                num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
                result[i] = letters[num.Int64()]
        }
        return string(result)
}

func waitForPodRunning(podName, namespace string) {
        for {
                out, _ := exec.Command("kubectl", "get", "pod", podName, "-n", namespace, "-o", "jsonpath={.status.phase}").Output()
                if strings.TrimSpace(string(out)) == "Running" {
                        log.Printf("Pod %s is now running.", podName)
                        break
                }
                time.Sleep(5 * time.Second)
        }
}

func waitForPodSuccess(podName, namespace string) {
        for {
                out, _ := exec.Command("kubectl", "get", "pod", podName, "-n", namespace, "-o", "jsonpath={.status.phase}").Output()
                if strings.TrimSpace(string(out)) == "Succeeded" {
                        log.Printf("Pod %s completed successfully.", podName)
                        break
                }
                time.Sleep(5 * time.Second)
        }
}

func deletePVC(clientset *kubernetes.Clientset, name, namespace string) {
        err := clientset.CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
        if err != nil {
                log.Fatalf("Error deleting PVC %s: %v", name, err)
        }
        log.Printf("Deleted PVC %s", name)
}

func createPVC(clientset *kubernetes.Clientset, name string, size int64, storageClass, namespace string) {
        storageQuantity := size * 1024 * 1024 * 1024

        pvc := &corev1.PersistentVolumeClaim{
                ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
                Spec: corev1.PersistentVolumeClaimSpec{
                        AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
                        Resources: corev1.VolumeResourceRequirements{
                                Requests: corev1.ResourceList{"storage": *resource.NewQuantity(storageQuantity, resource.DecimalSI)},
                        },
                        StorageClassName: &storageClass,
                },
        }

        _, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
        if err != nil {
                log.Fatalf("Error creating PVC %s: %v", name, err)
        }
        log.Printf("Created PVC %s", name)
}

func startSourcePod(pvc, namespace, podName string, sshPort int, username, password string) {
        log.Printf("Starting source pod %s with PVC %s", podName, pvc)

        exec.Command("kubectl", "delete", "pod", podName, "-n", namespace, "--ignore-not-found=true").Run()

        podYAML := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  containers:
  - name: ssh-server
    image: alpine:latest
    command: ["/bin/sh", "-x", "-c"]
    args:
      - |
        apk add --no-cache openssh rsync &&
        echo "%s:%s" | chpasswd &&
        sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config &&
        ssh-keygen -A &&
        /usr/sbin/sshd -D -e -p %d
    volumeMounts:
    - name: src-volume
      mountPath: /mnt/data
  restartPolicy: Always
  volumes:
  - name: src-volume
    persistentVolumeClaim:
      claimName: %s
`, podName, namespace, username, password, sshPort, pvc)

        cmd := exec.Command("kubectl", "apply", "-f", "-")
        cmd.Stdin = strings.NewReader(podYAML)
        err := cmd.Run()
        if err != nil {
                log.Fatalf("Error creating source pod: %v", err)
        }
}

func startDestPod(pvc, namespace, podName, srcPod string, bwLimit int, sshPort int, username, password string) {
        log.Printf("Starting destination pod %s with PVC %s", podName, pvc)

        exec.Command("kubectl", "delete", "pod", podName, "-n", namespace, "--ignore-not-found=true").Run()

        srcPodIP, err := exec.Command("kubectl", "-n", namespace, "get", "pod", srcPod, "-o", "jsonpath={.status.podIP}").Output()
        if err != nil {
                log.Fatalf("Error getting IP of source pod %s: %v", srcPod, err)
        }

        // sshpass -p %s rsync -aHAXvzc --bwlimit=%d --progress -e "ssh -p %d -o StrictHostKeyChecking=no" %s@%s:/mnt/data/ /mnt/data/
        podYAML := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  containers:
  - name: rsync-client
    image: alpine:latest
    command: ["/bin/sh", "-x", "-c"]
    args:
      - |
        apk add --no-cache rsync openssh-client sshpass &&
        sshpass -p %s rsync -aHAXSv --bwlimit=%d --progress -e "ssh -p %d -o StrictHostKeyChecking=no" %s@%s:/mnt/data/ /mnt/data/
    volumeMounts:
    - name: dst-volume
      mountPath: /mnt/data
  restartPolicy: Never
  volumes:
  - name: dst-volume
    persistentVolumeClaim:
      claimName: %s
`, podName, namespace, password, bwLimit, sshPort, username, strings.TrimSpace(string(srcPodIP)), pvc)

        cmd := exec.Command("kubectl", "apply", "-f", "-")
        cmd.Stdin = strings.NewReader(podYAML)
        err = cmd.Run()
        if err != nil {
                log.Fatalf("Error creating destination pod: %v", err)
        }
}

func scaleWorkload(clientset *kubernetes.Clientset, kind, name, namespace string, replicas int32) {
        switch kind {
        case "Deployment":
                _, err := clientset.AppsV1().Deployments(namespace).Patch(context.TODO(), name, types.StrategicMergePatchType,
                        []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)), metav1.PatchOptions{})
                if err != nil {
                        log.Fatalf("Failed to scale Deployment %s: %v", name, err)
                }
                log.Printf("Scaled Deployment %s to %d replicas", name, replicas)

        case "StatefulSet":
                _, err := clientset.AppsV1().StatefulSets(namespace).Patch(context.TODO(), name, types.StrategicMergePatchType,
                        []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)), metav1.PatchOptions{})
                if err != nil {
                        log.Fatalf("Failed to scale StatefulSet %s: %v", name, err)
                }
                log.Printf("Scaled StatefulSet %s to %d replicas", name, replicas)

        case "DaemonSet":
                err := clientset.AppsV1().DaemonSets(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
                if err != nil {
                        log.Fatalf("Failed to delete DaemonSet %s: %v", name, err)
                }
                log.Printf("Deleted DaemonSet %s", name)

        default:
                log.Printf("Unsupported workload type: %s", kind)
        }
}

func getPodLogs(clientset *kubernetes.Clientset, podName string, namespace string) error {

        cmd := exec.Command("kubectl", "logs", podName, "-n", namespace)

        // Configure stdout and stderr to go to the program's stdout and stderr
        cmd.Stdout = os.Stdout
        cmd.Stderr = os.Stderr

        err := cmd.Run()
        if err != nil {
                return fmt.Errorf("error running kubectl logs: %v", err)
        }
        return nil
}

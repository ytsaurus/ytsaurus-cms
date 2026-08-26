# YT Cluster Management System in K8S
## Installing
```
git clone https://github.com/ytsaurus/ytsaurus-cms.git
nano ytsaurus-cms/configs/k8s-cms.yaml # Fill 'proxy' field in cms config with cluster address

kubectl apply -f ytsaurus-cms/configs/k8s-cms.yaml -n <namespace>
```

YT_TOKEN for user `robot-yt-cms` must be placed to secret `cms` with key `YT_TOKEN`. `robot-yt-cms` must be `admins` group member.

## Docs
CMS creates "tasks" on k8s-nodes according to their [annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/). When CMS starts processing k8s-node, it annotates it with key `yt-cms/status` and periodically updates annotation value with corresponding task' `processing_state` (`new`, `pending`, `decomissioned`, `processed`). When node is annotated with `yt-cms/status: "processed"`, it can be taken safely. CMS' tasks and annotations are deleted when task reason (annotation) is deleted. 

### Creating tasks
Task can be created by annotation with key prefix `yt-cms-request`. For example, `yt-cms-request/any-issuer: "Comment"`.
Annotation's key becomes `task.Failure`, in this case `yt-cms-request/any-issuer`.
Annotation's value becomes `task.Comment`, in this case `Comment`.
`task.Action` will be `reboot`.

The simplest way to create task is annotation `yt-cms-request`.  Kubectl command (comment can be set in quotes):
```
kubectl annotate node my-node yt-cms-request=""
```

Kubectl command for task removing:
```
kubectl annotate node my-node yt-cms-request-
```
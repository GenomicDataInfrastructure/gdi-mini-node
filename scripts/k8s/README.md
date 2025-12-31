Kubernetes Deployment
=====================

This directory contains sample Kubernetes manifests for deploying gdi-mini-node.
Take it as a template to be customised for your needs.

More explicitly, the following additional steps are required at minimum:
1. copy configuration files to [config](./config) and customise them according
   to your deployment environment.
2. review and modify [ingress.yaml](ingress.yaml) according to you environment.
3. review image version in [deployment.yaml](deployment.yaml)

To deploy the Kubernetes manifests to your namespace:

```shell
kubectl apply -n your-namespace  -k . 
```

Here the final period sign here refers to this directory.

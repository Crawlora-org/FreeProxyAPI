provider "spot" {
  token = var.rackspace_spot_token
}

locals {
  nodepool_labels = {
    "app.kubernetes.io/name"       = "freeproxyapi"
    "app.kubernetes.io/managed-by" = "terraform"
  }
}

resource "spot_cloudspace" "freeproxyapi" {
  name               = var.cloudspace_name
  region             = var.region
  kubernetes_version = var.kubernetes_version
  cni                = var.cni
  hacontrol_plane    = var.ha_control_plane
  wait_until_ready   = var.wait_until_ready
  preemption_webhook = var.preemption_webhook
}

resource "spot_spotnodepool" "workers" {
  cloudspace_name      = spot_cloudspace.freeproxyapi.name
  server_class         = var.server_class
  bid_price            = var.bid_price
  desired_server_count = var.desired_nodes
  labels               = local.nodepool_labels
}

resource "terraform_data" "kubeconfig" {
  triggers_replace = [
    spot_cloudspace.freeproxyapi.name,
    spot_spotnodepool.workers.name,
    var.cloudspace_name,
    var.rackspace_organization_name,
  ]

  provisioner "local-exec" {
    command = "python3 ${path.module}/refresh_kubeconfig.py --terraform-dir ${path.module} --terraform-cmd terraform --organization ${var.rackspace_organization_name} --cloudspace-name ${var.cloudspace_name} --skip-terraform-refresh --no-verify"
  }
}

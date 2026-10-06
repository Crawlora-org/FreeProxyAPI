output "cloudspace_name" {
  description = "Provisioned Rackspace Spot cloudspace name."
  value       = spot_cloudspace.freeproxyapi.name
}

output "cloudspace_region" {
  description = "Provisioned cloudspace region."
  value       = spot_cloudspace.freeproxyapi.region
}

output "nodepool_name" {
  description = "Provisioned worker node pool name."
  value       = spot_spotnodepool.workers.name
}

output "kubeconfig_path" {
  description = "Local kubeconfig written by the post-apply refresh."
  value       = "${path.module}/kubeconfig.yaml"
}

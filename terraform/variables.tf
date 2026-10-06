variable "rackspace_spot_token" {
  description = "Rackspace Spot Terraform access token. Supply via TF_VAR_rackspace_spot_token or a local tfvars file."
  type        = string
  sensitive   = true
}

variable "rackspace_organization_name" {
  description = "Rackspace Spot organization used to mint kubeconfig."
  type        = string
  default     = "tony"
}

variable "cloudspace_name" {
  description = "Unique name for this regional Rackspace Spot cloudspace."
  type        = string
  default     = "freeproxyapi-dfw"
}

variable "region" {
  description = "Rackspace Spot region for the cloudspace."
  type        = string
  default     = "us-central-dfw-1"
}

variable "kubernetes_version" {
  description = "Kubernetes version to provision."
  type        = string
  default     = "1.32.9"
}

variable "cni" {
  description = "Rackspace Spot CNI."
  type        = string
  default     = "calico"
}

variable "ha_control_plane" {
  description = "Whether to enable the high-availability control plane."
  type        = bool
  default     = false
}

variable "wait_until_ready" {
  description = "Whether Terraform waits for the cloudspace control plane to be ready."
  type        = bool
  default     = true
}

variable "preemption_webhook" {
  description = "Optional Rackspace Spot preemption webhook URL."
  type        = string
  default     = null
  nullable    = true
}

variable "server_class" {
  description = "Rackspace Spot worker server class."
  type        = string
  default     = "mh.vs1.medium-dfw"
}

variable "bid_price" {
  description = "Hourly spot bid ceiling in USD per worker."
  type        = number
  default     = 0.01
}

variable "desired_nodes" {
  description = "Fixed number of worker nodes."
  type        = number
  default     = 4
}

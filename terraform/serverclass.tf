data "spot_serverclass" "workers" {
  name = var.server_class
}

output "worker_server_class" {
  description = "Resolved worker server class and current Spot catalog status."
  value = {
    name             = data.spot_serverclass.workers.name
    region           = data.spot_serverclass.workers.region
    availability     = data.spot_serverclass.workers.availability
    flavor_type      = data.spot_serverclass.workers.flavor_type
    cpu              = data.spot_serverclass.workers.resources.cpu
    memory           = data.spot_serverclass.workers.resources.memory
    available        = data.spot_serverclass.workers.status.available
    capacity         = data.spot_serverclass.workers.status.capacity
    market_price_usd = data.spot_serverclass.workers.status.spot_pricing.market_price_per_hour
  }
}

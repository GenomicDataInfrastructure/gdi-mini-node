from mini_node.beacon.config import BeaconCommonConfig, BeaconConfig, \
    BeaconContext
from mini_node.fdp.config import FdpConfig
from mini_node.setup import load_config_yaml

fdp_config: FdpConfig = load_config_yaml("fdp.yaml", FdpConfig)

model_common = load_config_yaml("beacon-common.yaml", BeaconCommonConfig)
model_aggregated = load_config_yaml("beacon-aggregated.yaml", BeaconConfig)
model_sensitive = load_config_yaml("beacon-sensitive.yaml", BeaconConfig)

if model_aggregated:
    beacon_aggregated = BeaconContext(model_common, model_aggregated, True)
else:
    beacon_aggregated = None

if model_sensitive:
    beacon_sensitive = BeaconContext(model_common, model_sensitive, False)
else:
    beacon_aggregated = None

del model_aggregated
del model_sensitive

def config(self, hosts):
    return MConfig.MConfig(hosts)


class MConfig:
    def __init__(self, hosts):
        self.host_seed = hosts
        self.replicaset = None
        self.database = "test"

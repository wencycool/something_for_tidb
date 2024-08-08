# 反亲和检查

## 背景

TiDB集群较多部署在虚拟化环境中，当虚拟机的宿主机发生故障后，其上面的TiDB节点会发生漂移，导致多个TiDB节点部署在同一物理机上，这样会导致TiDB集群的高可用性降低。

---

## 设计思路

针对`tidb`,`pd`,`tikv`,`tiflash`四种组件，通过反亲和规则，将同一组件的节点尽量分散部署在不同的物理机上，保证两点：

1. 一类组件的节点满足反亲和规则，尽量分散部署在不同的物理机上。
2. 为满足性能需求，一类组件的节点尽可能均匀分布在不同的物理机上。

### tidb节点

tidb节点是无状态的，由前端负载均衡器负责请求的分发，因此并不存在副本反亲和的问题（不考虑follow read方案），总体做如下处理：

- **最小部署原则**：tidb节点至少分布在两个物理机上，保证一台物理机故障后，集群仍然可用。
- **均衡原则**：M个tidb节点，N个物理机，那么每台物理机上的tidb节点数目不超过M/N+1。

### pd节点

pd节点是有状态的，一个pd节点代表一个副本，因此需要保证副本反亲和，总体做如下处理：

- **最小部署原则**：pd节点数目至少为3个(_建议pd节点数=3_)。
- **均衡原则**：M个pd节点，N个物理机，那么每台物理机上的pd节点数目一定为1。
- **副本反亲和原则**：一个物理机上只能有一个pd节点。

### tikv节点

tikv节点是有状态的，相比tidb无状态，pd每一个节点代表一个副本，tikv相对来说更加复杂，因此tikv的副本划分并不是节点级，而是“块”级（region），
因此无法通过节点级别的反亲和来保证副本反亲和。需要通过集群拓扑的labels来保证副本反亲和，参考如下两篇文章：
1. [通过拓扑 label 进行副本调度](https://docs.pingcap.com/zh/tidb/stable/schedule-replicas-by-topology-labels)
2. [TiDB 数据库的调度](https://docs.pingcap.com/zh/tidb/stable/tidb-scheduling)

总体上来说是通过pd的配置参数：location-labels来让tidb集群感知集群高可用拓扑，pd的配置参数：isolation-level来让TiDB集群自身保证副本反亲和，
tikv的配置参数：labels来让pd感知集群高可用拓扑。这样一来，TiDB集群就可以保证副本反亲和。我们只需要tikv节点所在宿主机符合TiDB集群自身的反亲和规则即可。

location-labels可以表示：["zone", "rack","host"]等多种拓扑层级，让pd感知这种拓扑并进行调度。  
isolation-level值选自location-labels，表示TiDB 集群的最小强制拓扑隔离级别，TiDB 集群会自动保证一个 region 的多个副本不会分布在同一个 isolation-level 层级中。  
例如：  
location-labels=["zone", "rack","host"]，isolation-level="rack"，表示TiDB 集群会自动保证一个 region 的多个副本不会分布在同一个rack中。  
如果isolation-level为默认值（空），表示TiDB 集群会自动保证一个 region 的多个副本不会分布在同一个节点上。  

因此只需要保证tikv节点所在物理机符合TiDB集群自身的反亲和规则即可，即物理机在isolated-level的最低层级或者更低层级，总体做如下处理：

- **最小部署原则**：tikv节点至少分布在等于副本数个数的物理机上，如：最常用的3副本，那么宿主机应该至少有3个。
- **均衡原则**：M个tikv节点，N个物理机，那么每台物理机上的tikv节点数目不超过M/N+1。
- **副本反亲和原则**：每一个tikv节点的label利用isolation-level查找到tidb反亲和的层级，这个层级标记在同一个物理机上必须相同。

### tiflash节点

整体上参考tikv的反亲和设计原则，但tiflash副本数可以是任意数（生产上一般设置2副本），总体做如下处理：

- **最小部署原则**：tiflash节点至少分布在等于副本数个数的物理机上，如：最常用的2副本，那么宿主机应该至少有2个。
- **均衡原则**：M个tiflash节点，N个物理机，那么每台物理机上的tiflash节点数目不超过M/N+1。
- **副本反亲和原则**：每一个tiflash节点的label利用isolation-level查找到tidb反亲和的层级，这个层级标记在同一个物理机上必须相同。

## 大致实现方案

### 通过API方式获取tikv的拓扑标签信息

pd节点`location-labels`和`isolation-level`的配置获取方式：
```html
http://<host>:2379/pd/api/v1/config
```
tikv节点的labels获取方式：
```html
http://<host>:20180/config
```

### 通过tiup管控机获取集群节点信息

### 提供虚拟机和宿主机的映射关系

### 得到每个集群哪些节点类型存在反亲和问题
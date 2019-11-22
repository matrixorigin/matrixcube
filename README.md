# Beehive
Beehive是一个用于构建Multi-Raft的强一致系统的类库。应用程序完全不需要关心分布式的问题，只需要编写自己的数据存储的单机代码即可。

## 特性
* Strong consistent persistence storage
* High availability
* Horizontal scalability
* Auto Rebalance

## Quick start
### 一个基于Redis协议的存储服务
例子代码在 [这里](./example/redis) 

```bash
make example-redis
docker-compose up
```

启动完毕后，启动了一个4个节点的集群，4个节点都可以对外提供Redis的服务，任何一个节点对于客户端都是等价的。4个节点监听`6371~6374`四个端口。
利用`redis-cli`来连接任意一个节点来试试吧。

### 一个基于http协议的自定义存储服务
例子代码在 [这里](./example/http) 

```bash
make example-http
docker-compose -f ./docker-compose-http up
```

启动完毕后，启动了一个4个节点的集群，4个节点都可以对外提供http的服务，任何一个节点对于客户端都是等价的。4个节点监听`6371~6374`四个端口。

```bash
curl "http://127.0.0.1:6371/set?key=k&value=v"

curl "http://127.0.0.1:6371/get?key=k"

curl "http://127.0.0.1:6371/delete?key=k"
```

# kafkashow

Kafka 终端查看工具（TUI），用 Go 编写，支持在终端中浏览 Topic、分区、消息和 Consumer Group。

仓库地址：[https://github.com/showx/kafkashow](https://github.com/showx/kafkashow)

## 功能

- 连接 Kafka 集群（支持多 broker）
- 浏览 Topic 列表及分区数
- 查看分区详情（Leader、副本、Offset 范围）
- 消费并查看最近消息
- 浏览 Consumer Group 及成员信息

## 安装

```bash
go install github.com/showx/kafkashow@latest
```

或克隆仓库后本地编译：

```bash
git clone https://github.com/showx/kafkashow.git
cd kafkashow
go build -o kafkashow .
```

## 使用

```bash
# 直接运行
go run .

# 或运行编译后的二进制
./kafkashow
```

启动后输入 broker 地址（如 `localhost:9092`），多个 broker 用逗号分隔。

## 快捷键

| 按键 | 功能 |
|------|------|
| `j` / `↓` | 下移 |
| `k` / `↑` | 上移 |
| `Enter` | 确认 / 进入详情 |
| `m` | 查看消息（分区详情页） |
| `Tab` | 切换 Topics / Groups |
| `r` | 刷新当前视图 |
| `Esc` | 返回上一级 |
| `?` | 显示帮助 |
| `q` | 退出 |

## 技术栈

- [Bubble Tea](https://github.com/charmbracelet/bubbletea) — TUI 框架
- [kafka-go](https://github.com/segmentio/kafka-go) — Kafka 客户端（纯 Go，无 CGO）

## 本地测试

可用 Docker 快速启动 Kafka：

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:latest
```

## License

MIT

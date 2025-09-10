package main

import (
	"encoding/json"
	"fmt"
	"kafka-lag/push_metrics"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/apolloconfig/agollo/v4"
	"github.com/apolloconfig/agollo/v4/env/config"
	"github.com/robfig/cron"
)

func main() {
	ns := os.Getenv("ns")
	if ns == "" {
		ns = "application"
	}
	appConfig := &config.AppConfig{
		AppID:          "kafka-monitor",
		Cluster:        "PRO",
		NamespaceName:  ns,
		IP:             "https://XXXXXXXXXXXXXXXXXXX",
		Secret:         "XXXXXXXXXXXXXXXX",
		IsBackupConfig: true,
	}
	// 创建 Apollo 客户端
	apollo_client, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
		return appConfig, nil
	})
	if err != nil {
		log.Printf("Error starting Apollo client: %s", err)
		return
	}

	kafka_config := apollo_client.GetConfigCache(appConfig.NamespaceName)
	if kafka_config == nil {
		log.Printf("Error: Config cache is nil for namespace '%s'", appConfig.NamespaceName)
		return
	}
	kafka_cluster, err := kafka_config.Get("kafka_list")
	if err != nil {
		log.Fatal(err)
		return
	}
	var namespaces []string
	if str, ok := kafka_cluster.(string); ok {
		err := json.Unmarshal([]byte(str), &namespaces)
		if err != nil {
			log.Printf("解析 JSON 失败: %s", err)
			return
		}
	} else {
		log.Printf("转换失败，kafka_cluster 不是 string 类型")
	}
	c := cron.New()
	c.AddFunc("@every 60s", func() {
		log.Printf("开始执行获取kafka Lag任务...")
		Get_kafka_info(apollo_client, namespaces)
	})
	c.Start()
	select {}
}

func Get_kafka_info(apollo_client agollo.Client, namespaces []string) {
	kafkaInstances := make(map[string]sarama.Client)
	for _, namespace := range namespaces {
		kafkaConfigMap := make(map[string]string)
		configCache := apollo_client.GetConfigCache(namespace)
		if configCache == nil {
			log.Printf("Error: Config cache is nil for namespace '%s'", namespace)
			continue
		}
		configCache.Range(func(key, value interface{}) bool {
			kafkaConfigMap[key.(string)] = value.(string)
			return true
		})

		//字符串类型转换
		saslEnable, _ := strconv.ParseBool(kafkaConfigMap["kafka.sasl.enable"])
		kafkaVersionStr := kafkaConfigMap["version"]
		var matchedVersion sarama.KafkaVersion
		versionFound := false
		for _, version := range sarama.SupportedVersions {
			if version.String() == kafkaVersionStr {
				matchedVersion = version
				versionFound = true
				break
			}
		}
		if !versionFound {
			log.Printf("Error: Unsupported Kafka version: %s", kafkaVersionStr)
			return
		}
		//初始化配置
		brokers := kafkaConfigMap["kafka.brokers"]
		kafkaConfig := sarama.NewConfig()
		kafkaConfig.Net.SASL.Enable = saslEnable
		kafkaConfig.Net.SASL.User = kafkaConfigMap["kafka.sasl.user"]
		kafkaConfig.Net.SASL.Password = kafkaConfigMap["kafka.sasl.password"]
		kafkaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(kafkaConfigMap["kafka.sasl.mechanism"])
		kafkaConfig.Version = matchedVersion

		// 创建 Kafka 客户端
		kafka_client, err := sarama.NewClient([]string{brokers}, kafkaConfig)
		if err != nil {
			log.Fatalf("Error creating Kafka client: %v", err)
		}
		defer kafka_client.Close()

		kafkaInstances[namespace] = kafka_client
		//遍历所有命名空间下所有kafka信息
	}
	var wg sync.WaitGroup
	for namespace, client := range kafkaInstances {
		admin, err := sarama.NewClusterAdminFromClient(client)
		if err != nil {
			log.Fatalf("Error creating Kafka admin: %v", err)
		}
		defer admin.Close()
		groups, err := admin.ListConsumerGroups()
		if err != nil {
			log.Fatalf("Error listing consumer groups: %v", err)
		}
		// 遍历每个消费者组并使用 goroutine 并发处理
		for group := range groups {
			wg.Add(1)
			go func(group string) {
				defer wg.Done()
				err := calculateGroupLag(admin, group, client, namespace)
				if err != nil {
					log.Printf("Error calculating lag for group %s: %v\n", group, err)
				}
			}(group)
		}
	}
	wg.Wait()

}

func calculateGroupLag(admin sarama.ClusterAdmin, group string, client sarama.Client, namespace string) error {
	// defer wg.Done()
	partitionsLag := make(map[string]int64)

	// 获取消费者组的描述信息
	groupDescription, err := admin.DescribeConsumerGroups([]string{group})
	if err != nil {
		return err
	}
	// 创建一个 OffsetManager 用于整个消费者组
	offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		return err
	}
	defer offsetManager.Close()
	for _, description := range groupDescription {
		for _, member := range description.Members {
			assignment, err := member.GetMemberAssignment()
			if err != nil {
				return err
			}
			if assignment == nil || assignment.Topics == nil {
				log.Printf("Error: assignment is nil for member %v", assignment)
				continue
			}
			for topic, partitions := range assignment.Topics {
				for _, partition := range partitions {
					// 获取分区的高水位线
					partitionOffsets, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
					if err != nil {
						return err
					}

					// 使用已存在的 OffsetManager 获取已提交的 offset
					partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
					if err != nil {
						return err
					}
					if partitionOffsetManager == nil {
						// 处理partitionOffsetManager为nil的情况，例如记录错误日志或设置默认值
						log.Printf("Error: partitionOffsetManager is nil for topic %s, partition %d in group %s\n", topic, partition, group)
						continue
					}
					//依次关闭partitionOffsetManager分区连接时，会有接近一秒的关闭时间.后续优化
					defer partitionOffsetManager.Close()

					consumerOffset, _ := partitionOffsetManager.NextOffset()
					if consumerOffset == -1 {
						consumerOffset = partitionOffsets
					}

					// 计算 lag
					lag := partitionOffsets - consumerOffset
					partitionsLag[topic] += lag

				}
			}
		}
	}
	for topic, lag := range partitionsLag {
		metricStr := fmt.Sprintf(`kafka_lag{namespace="%s", Consumer_Group="%s", Topic="%s"} %d`, namespace, group, topic, lag)
		// log.Printf("%s", metricStr)
		push_metrics.Push_metrics(metricStr)
	}
	return nil
}

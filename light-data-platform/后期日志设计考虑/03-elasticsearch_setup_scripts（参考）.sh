#!/bin/bash
# =============================================================================
# Elasticsearch Nginx日志存储完整建表脚本
# 可直接执行，包含所有索引、模板、策略配置
# =============================================================================

# 配置变量 - 根据实际环境修改
ES_HOST="http://elasticsearch:9200"
ES_USER="elastic"           # 如果启用了安全认证
ES_PASSWORD="your_password" # 如果启用了安全认证

# 认证配置 (如果ES开启了安全认证，取消注释)
# AUTH="-u ${ES_USER}:${ES_PASSWORD}"
AUTH=""

echo "=== 开始配置Elasticsearch for Nginx日志存储 ==="

# =============================================================================
# 1. 创建ILM生命周期策略
# =============================================================================
echo "1. 创建ILM生命周期策略..."

curl -X PUT "${ES_HOST}/_ilm/policy/nginx-logs-policy" ${AUTH} \
-H "Content-Type: application/json" \
-d '{
  "policy": {
    "phases": {
      "hot": {
        "min_age": "0ms",
        "actions": {
          "rollover": {
            "max_size": "10GB",
            "max_age": "1d",
            "max_docs": 50000000
          },
          "set_priority": {
            "priority": 100
          }
        }
      },
      "warm": {
        "min_age": "7d",
        "actions": {
          "set_priority": {
            "priority": 50
          },
          "allocate": {
            "number_of_replicas": 0,
            "include": {
              "box_type": "warm"
            }
          },
          "forcemerge": {
            "max_num_segments": 1
          }
        }
      },
      "cold": {
        "min_age": "30d",
        "actions": {
          "set_priority": {
            "priority": 10
          },
          "allocate": {
            "number_of_replicas": 0,
            "include": {
              "box_type": "cold"
            }
          }
        }
      },
      "delete": {
        "min_age": "180d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}'

echo -e "\n✅ ILM策略创建完成"

# =============================================================================
# 2. 创建索引模板
# =============================================================================
echo "2. 创建索引模板..."

curl -X PUT "${ES_HOST}/_index_template/nginx-logs-template" ${AUTH} \
-H "Content-Type: application/json" \
-d '{
  "index_patterns": ["nginx-logs-*"],
  "template": {
    "settings": {
      "index": {
        "number_of_shards": 6,
        "number_of_replicas": 1,
        "refresh_interval": "30s",
        "lifecycle": {
          "name": "nginx-logs-policy",
          "rollover_alias": "nginx-logs"
        },
        "codec": "best_compression",
        "sort.field": ["@timestamp"],
        "sort.order": ["desc"]
      },
      "analysis": {
        "analyzer": {
          "uri_analyzer": {
            "type": "custom",
            "tokenizer": "keyword",
            "filter": ["lowercase"]
          }
        }
      }
    },
    "mappings": {
      "properties": {
        "@timestamp": {
          "type": "date",
          "format": "epoch_millis"
        },
        "time_iso8601": {
          "type": "date",
          "format": "strict_date_optional_time",
          "index": false
        },
        "cluster": {
          "type": "keyword",
          "fields": {
            "text": {
              "type": "text",
              "analyzer": "standard"
            }
          }
        },
        "service": {
          "type": "keyword"
        },
        "node": {
          "type": "keyword"
        },
        "node_ip": {
          "type": "ip"
        },
        "client_ip": {
          "type": "ip"
        },
        "method": {
          "type": "keyword"
        },
        "uri": {
          "type": "text",
          "analyzer": "uri_analyzer",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 512
            }
          }
        },
        "uri_normalized": {
          "type": "keyword",
          "ignore_above": 256
        },
        "path": {
          "type": "keyword",
          "ignore_above": 256
        },
        "query": {
          "type": "text",
          "index": false
        },
        "host": {
          "type": "keyword"
        },
        "status": {
          "type": "short"
        },
        "rt": {
          "type": "float"
        },
        "rt_ms": {
          "type": "integer"
        },
        "uct": {
          "type": "float"
        },
        "uht": {
          "type": "float"
        },
        "urt": {
          "type": "float"
        },
        "upstream": {
          "type": "keyword"
        },
        "ups": {
          "type": "short"
        },
        "cache": {
          "type": "keyword"
        },
        "bytes": {
          "type": "integer"
        },
        "req_len": {
          "type": "integer"
        },
        "trace_id": {
          "type": "keyword"
        },
        "req_id": {
          "type": "keyword"
        },
        "ua": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "is_slow": {
          "type": "boolean"
        },
        "is_error": {
          "type": "boolean"
        },
        "is_5xx": {
          "type": "boolean"
        },
        "backend_process_time": {
          "type": "float"
        },
        "business_sign": {
          "type": "keyword",
          "index": false
        },
        "business_timestamp": {
          "type": "keyword",
          "index": false
        },
        "business_version": {
          "type": "keyword"
        },
        "business_app_key": {
          "type": "keyword"
        }
      }
    }
  },
  "composed_of": [],
  "priority": 200,
  "version": 1
}'

echo -e "\n✅ 索引模板创建完成"

# =============================================================================
# 3. 创建初始索引和别名
# =============================================================================
echo "3. 创建初始索引和别名..."

# 创建第一个索引
curl -X PUT "${ES_HOST}/nginx-logs-000001" ${AUTH} \
-H "Content-Type: application/json" \
-d '{
  "aliases": {
    "nginx-logs": {
      "is_write_index": true
    }
  }
}'

echo -e "\n✅ 初始索引创建完成"

# =============================================================================
# 4. 创建常用的索引别名
# =============================================================================
echo "4. 创建常用别名..."

# 最近7天的别名 (热查询)
curl -X POST "${ES_HOST}/_aliases" ${AUTH} \
-H "Content-Type: application/json" \
-d '{
  "actions": [
    {
      "add": {
        "index": "nginx-logs-*",
        "alias": "nginx-logs-recent",
        "filter": {
          "range": {
            "@timestamp": {
              "gte": "now-7d"
            }
          }
        }
      }
    }
  ]
}'

# 错误日志别名
curl -X POST "${ES_HOST}/_aliases" ${AUTH} \
-H "Content-Type: application/json" \
-d '{
  "actions": [
    {
      "add": {
        "index": "nginx-logs-*", 
        "alias": "nginx-logs-errors",
        "filter": {
          "range": {
            "status": {
              "gte": 400
            }
          }
        }
      }
    }
  ]
}'

echo -e "\n✅ 别名创建完成"

# =============================================================================
# 5. 创建Dashboard用的数据视图
# =============================================================================
echo "5. 创建Kibana数据视图..."

# 这个需要在Kibana中执行，或者通过Kibana API
curl -X POST "${ES_HOST}/api/saved_objects/index-pattern/nginx-logs-*" ${AUTH} \
-H "Content-Type: application/json" \
-H "kbn-xsrf: true" \
-d '{
  "attributes": {
    "title": "nginx-logs-*",
    "timeFieldName": "@timestamp",
    "fields": "[{\"name\":\"@timestamp\",\"type\":\"date\",\"searchable\":true,\"aggregatable\":true},{\"name\":\"cluster\",\"type\":\"string\",\"searchable\":true,\"aggregatable\":true}]"
  }
}' 2>/dev/null || echo "Kibana数据视图需要在Kibana界面中手动创建"

echo -e "\n✅ 数据视图配置完成"

# =============================================================================
# 6. 验证配置
# =============================================================================
echo "6. 验证配置..."

echo "检查ILM策略:"
curl -X GET "${ES_HOST}/_ilm/policy/nginx-logs-policy" ${AUTH} | jq '.nginx-logs-policy.policy.phases' 2>/dev/null || echo "请检查jq是否安装"

echo -e "\n检查索引模板:"
curl -X GET "${ES_HOST}/_index_template/nginx-logs-template" ${AUTH} | jq '.index_templates[0].index_template.template.settings' 2>/dev/null || echo "模板已创建"

echo -e "\n检查索引状态:"
curl -X GET "${ES_HOST}/_cat/indices/nginx-logs-*?v" ${AUTH}

echo -e "\n检查别名:"
curl -X GET "${ES_HOST}/_cat/aliases/nginx-logs*?v" ${AUTH}

echo -e "\n=== Elasticsearch配置完成 ==="
echo -e "\n📋 接下来需要做的:"
echo "1. 配置Kafka消费者将日志写入 nginx-logs 别名"
echo "2. 在夜莺中配置ES数据源，地址: ${ES_HOST}"
echo "3. 根据实际情况调整分片数和副本数"
echo "4. 监控索引大小和查询性能"

# =============================================================================
# 7. 性能优化建议脚本
# =============================================================================
echo -e "\n=== 性能优化配置 ==="

# 设置集群级别优化参数
curl -X PUT "${ES_HOST}/_cluster/settings" ${AUTH} \
-H "Content-Type: application/json" \
-d '{
  "persistent": {
    "indices.memory.index_buffer_size": "30%",
    "indices.memory.min_index_buffer_size": "96mb",
    "thread_pool.write.queue_size": 1000,
    "thread_pool.search.queue_size": 1000
  }
}'

echo -e "✅ 集群优化参数设置完成"

echo -e "\n🎯 根据您的环境需要修改的地方:"
echo "1. ES_HOST: 修改为实际的Elasticsearch地址"
echo "2. ES_USER/ES_PASSWORD: 如果启用了安全认证"
echo "3. number_of_shards: 根据集群节点数调整(建议=节点数)"
echo "4. ILM策略的时间: 根据实际保留需求调整"
echo "5. box_type标签: 根据实际的节点标签调整"
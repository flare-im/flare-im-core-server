#!/bin/bash

# 数据库迁移脚本
# 用于初始化或更新Flare IM Core数据库表结构

set -e

echo "开始执行数据库迁移..."

# 通过Docker执行初始化SQL脚本
echo "执行初始化脚本..."
docker exec -i flare-postgres psql -U flare -d flare2 < /Users/hg/workspace/flare/flare-im/flare-im-core/deploy/init.sql

echo "数据库迁移完成!"

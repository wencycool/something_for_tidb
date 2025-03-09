#!/bin/bash

# 设置Git代理
git config --global http.proxy http://127.0.0.1:7890
git config --global https.proxy http://127.0.0.1:7890

# 验证设置
echo "当前Git代理设置："
echo "HTTP代理: $(git config --global --get http.proxy)"
echo "HTTPS代理: $(git config --global --get https.proxy)"

# 测试连接
echo -e "\n测试GitHub连接..."
curl -I https://github.com 
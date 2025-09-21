# zzwork_etl

## 项目概述
这是一个综合性的数据ETL（Extract, Transform, Load）项目集合，包含多个客户项目的数据处理脚本和分析工具。项目采用模块化设计，便于管理和扩展。

## 🗂️ 项目结构
```
zzwork_etl/
├── README.md                    # 项目说明文档
├── datahub_amazon/             # ✅ Amazon电商数据ETL（已完成）
│   ├── dwd_sku_info.py         # Amazon SKU数据ETL脚本（基础版本）
│   └── dwd_sku_info_prod.py    # Amazon SKU数据ETL脚本（生产版本）
├── datahub_ali/                # 🔄 阿里数据中心相关脚本
├── mode_test/                  # 🧪 测试和验证脚本
├── ms_aowei_cloud/             # 📊 奥威云项目
├── ms_bayer/                   # 🌾 拜耳项目数据处理
├── ms_evereden/                # 🧴 Evereden项目
├── ms_mars/                    # 🐕 Mars项目
├── ms_nice/                    # 🛍️ Nice项目数据处理
├── ms_pg_video/                # 🎥 P&G视频处理项目
├── ms_scj/                     # 🧽 SCJ项目
├── ms_scj_alijd/               # 🛒 SCJ阿里京东项目
├── test/                       # 🔧 通用测试脚本
├── utils/                      # 🛠️ 通用工具函数库
└── docs/                       # 📚 项目文档
```

## 🚀 核心项目

### datahub_amazon - Amazon电商数据ETL ✅
**状态**: 已完成并投入生产使用

**核心脚本**:
- `dwd_sku_info.py` - 基础版Amazon SKU数据ETL脚本
- `dwd_sku_info_prod.py` - 生产版本，包含增强功能

**主要功能**:
- 🔄 多源数据整合（`ods_menu_v1`, `ods_menu_detail_v1`）
- 📊 动态分区写入（region + dt + etl_source）
- 🌐 多语言销量解析（英语、德语、西班牙语、法语、意大利语）
- 🔧 智能空格处理：`"1 k+ comprados"` → `1000`
- ⚡ 原子性数据更新（INSERT OVERWRITE）
- 📈 自动元数据刷新

**技术亮点**:
- 支持带空格的k/w格式解析
- 优化的region映射逻辑
- 生产级错误处理和日志记录

## 🛠️ 技术栈
- **PySpark**: 大数据处理框架
- **Hive/Impala**: 数据仓库和查询引擎
- **Python**: 主要编程语言
- **Git**: 版本控制

## 📋 开发计划

### 🎯 近期目标
- [ ] 添加数据质量验证脚本到 `utils/`
- [ ] 完善 `mode_test/` 中的测试工具
- [ ] 添加通用配置管理到 `utils/`

### 🔮 长期规划
- [ ] 各客户项目脚本逐步添加
- [ ] 建立统一的ETL开发规范
- [ ] 实现自动化测试框架
- [ ] 添加监控和告警机制

## 📖 使用指南

### 环境要求
- Python 3.6+
- PySpark 2.4+
- Hive/Impala集群访问权限

### 快速开始
1. 克隆仓库到本地
```bash
git clone https://github.com/Amada203/zzwork_etl.git
cd zzwork_etl
```

2. 根据具体项目需求选择对应脚本
3. 配置数据库连接参数
4. 运行ETL脚本

### 添加新脚本
1. 选择合适的项目文件夹
2. 添加脚本文件
3. 更新对应文件夹的README
4. 提交到Git仓库

## 🔒 安全说明
- 所有敏感信息（数据库密码、API密钥等）已从代码中移除
- 使用配置文件或环境变量管理敏感信息
- 定期检查代码中的安全漏洞

## 📊 项目统计
- **已完成项目**: 1个（datahub_amazon）
- **预留项目**: 11个
- **核心脚本**: 2个
- **最后更新**: 2025-09-21

## 👥 贡献指南
1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 推送到分支
5. 创建 Pull Request

## 📝 更新日志
- **v1.0.0** (2025-09-21): 初始版本，包含Amazon ETL脚本
- 修复销量解析空格问题
- 优化region映射逻辑
- 实现生产级错误处理

## 📞 联系信息
- **开发者**: Amada203
- **GitHub**: https://github.com/Amada203/zzwork_etl
- **创建时间**: 2025-09-21

---
*本项目持续更新中，欢迎提交Issue和Pull Request！*
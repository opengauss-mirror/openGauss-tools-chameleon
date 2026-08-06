# Pre-commit 使用说明

本仓库已配置 [pre-commit](https://pre-commit.com/)，在每次 `git commit` 前自动执行代码质量检查（空白符、YAML/JSON 校验、拼写、lint、格式化、安全检查等），帮助在本地尽早发现问题。

所有 pre-commit 工具仓库均从 **gitcode 镜像**拉取，无需访问 GitHub。

## 快速安装（推荐）

在仓库根目录执行：

```bash
bash setup-pre-commit.sh
```

脚本会将工具安装到仓库下的独立虚拟环境 `.pre-commit-venv/`，不污染系统 Python。看到 `完成！` 后即可正常提交。

## 手动安装

```bash
# 建议先建独立虚拟环境
python3 -m venv .pre-commit-venv
source .pre-commit-venv/bin/activate   # Windows: .pre-commit-venv\Scripts\activate

# 安装 pre-commit（-i 仅对本条命令临时指定源）
pip install -i https://mirrors.aliyun.com/pypi/simple/ pre-commit

# 安装 git 钩子
pre-commit install

# 预热钩子环境
PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
PIP_TRUSTED_HOST=mirrors.aliyun.com \
  pre-commit install-hooks
```

## 日常使用

| 操作 | 命令 |
|------|------|
| 正常提交（自动检查） | `git commit` |
| 手动全量检查 | `pre-commit run --all-files` |
| 检查指定文件 | `pre-commit run --files pg_chameleon/lib/pg_lib.py` |
| 跳过本次检查 | `git commit --no-verify` |

## 已启用的检查项

| 工具 | 作用 |
|------|------|
| pre-commit-hooks | 去除行尾空白、文件末尾换行、YAML/JSON/TOML 校验、大文件检测、私钥检测 |
| codespell | Markdown 拼写检查 |
| ruff | Python lint 并自动修复（目标 Python 3.5+） |
| bandit | Python 安全扫描 |
| darker | 仅格式化本次改动的 Python 行（配合 black + isort） |

SQL 脚本、图片等非 Python 源码目录已在配置中排除。

## 常见问题

**Q1. `install-hooks` 报 `Could not find a version that satisfies ... ruamel.yaml`？**

pre-commit 建环境时没走国内源。最省事是直接 `bash setup-pre-commit.sh`；手动的话用 `PIP_INDEX_URL` 环境变量指定国内源后执行 `pre-commit install-hooks`，必要时先 `pre-commit clean` 再重试。

**Q2. clone gitcode 很慢或失败？**

确认能访问 `https://gitcode.com`。本套配置所有工具源都指向 gitcode，不连 GitHub。

**Q3. 如何卸载？**

```bash
pre-commit uninstall          # 移除 git 钩子
rm -rf .pre-commit-venv/      # 删除隔离环境
```

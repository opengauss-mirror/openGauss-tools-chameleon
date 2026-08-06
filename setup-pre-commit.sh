#!/usr/bin/env bash
# =============================================================================
# setup-pre-commit.sh — openGauss-tools-chameleon
#
# 作用：在当前仓库为开发者装好提交前检查（pre-commit）。
#
# 设计原则（重要）：
#   * 不污染系统/全局环境：所有工具装进本仓库下的独立虚拟环境
#     .pre-commit-venv/（已建议加入 .gitignore），
#     不动系统 python、不写 `pip config set`、不写 ~/.config/pip/pip.conf。
#   * 工具源走 gitcode 镜像，避免直连 GitHub。
#
# 前提：能访问 gitcode.com；本机有 python>=3.10。
# =============================================================================
set -euo pipefail

log()  { printf '[setup-pre-commit] %s\n' "$*"; }
die()  { printf '[setup-pre-commit] 错误: %s\n' "$*" >&2; exit 1; }

command -v git >/dev/null 2>&1 || die "未找到 git"
git rev-parse --show-toplevel >/dev/null 2>&1 || die "当前不在 git 仓库内，请在仓库根目录运行"

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
[ -f .pre-commit-config.yaml ] || die "当前仓库没有 .pre-commit-config.yaml，无法安装"

# ---- 1. 创建隔离虚拟环境 ----
VENV_DIR="$REPO_ROOT/.pre-commit-venv"
PIP_MIRROR="https://mirrors.aliyun.com/pypi/simple/"
PIP_HOST="mirrors.aliyun.com"

if [ ! -d "$VENV_DIR" ]; then
  log "创建隔离虚拟环境: $VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

VENV_PY="$VENV_DIR/bin/python"
[ -x "$VENV_PY" ] || die "虚拟环境异常，请删除 $VENV_DIR 后重试"

# ---- 2. 在 venv 内安装 pre-commit ----
PKGS=(pre-commit)
log "安装隔离环境工具: ${PKGS[*]}"
"$VENV_PY" -m pip install --disable-pip-version-check -q \
  -i "$PIP_MIRROR" --trusted-host "$PIP_HOST" --upgrade pip
"$VENV_PY" -m pip install --disable-pip-version-check \
  -i "$PIP_MIRROR" --trusted-host "$PIP_HOST" \
  "${PKGS[@]}"

PRE_COMMIT="$VENV_DIR/bin/pre-commit"

# ---- 3. 安装 git 钩子 ----
# pre-commit install 会把 venv 里的 pre-commit 路径写进 .git/hooks/pre-commit，
# 因此 commit 时无需手动激活 venv，钩子会自动用这个隔离环境里的工具。
log "安装 git 钩子"
"$PRE_COMMIT" install

# ---- 4. 预热各钩子环境（源用环境变量临时指定）----
log "预热钩子环境，首次会联网从 gitcode 拉取，请稍候…"
PIP_INDEX_URL="$PIP_MIRROR" PIP_TRUSTED_HOST="$PIP_HOST" \
  "$PRE_COMMIT" install-hooks

log "完成！之后每次 git commit 会自动运行 pre-commit 检查。"
log "手动检查: $PRE_COMMIT run --all-files"
log "跳过检查: git commit --no-verify"

# 定义要删除的变量名（例如 LD_LIBRARY_PATH）
TARGET_VAR="LD_LIBRARY_PATH"
# 获取当前工作目录并转义特殊字符
CURRENT_DIR=$(pwd | sed 's/\//\\\//g')
# 从 ~/.bashrc 中删除 LD_LIBRARY_PATH=`pwd`:$LD_LIBRARY_PATH
# 使用单引号包裹正则表达式，避免 Shell 解析 $
sed -i '/^'"${TARGET_VAR}=${CURRENT_DIR}:\\\$${TARGET_VAR}"'/d' ~/.bashrc
# 从 ~/.bashrc 中删除 export LD_LIBRARY_PATH, 但不能删除 export LD_LIBRARY_PATH=xxx
sed -i "/^export ${TARGET_VAR}$/d" ~/.bashrc
# 提示信息
echo "The environment variable ${TARGET_VAR} has been removed from ~/.bashrc"

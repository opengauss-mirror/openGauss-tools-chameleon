venv_path=$(pwd)
chameleon_path=${venv_path}/venv/bin/chameleon
version=6.0.0
chameleon_version="chameleon "$version
version_str=''
install_log=install.log
echo $venv_path >> ${install_log}

function run_build(){
  echo "
  LD_LIBRARY_PATH=`pwd`:\$LD_LIBRARY_PATH
  export LD_LIBRARY_PATH
  " >> ~/.bashrc
  #解压后的操作
  cd venv/bin
  #创建软链接
  ln -s $venv_path/python3.6.8/bin/python3.6 python3.6
  #修改虚拟环境实际的python路径，流水线python3.6路径为/venv/bin/python3.6，该路径随流水线打包环境修改
  sed -i "1s#/venv/bin/python3.6#$venv_path/venv/bin/python3.6#" `grep /venv/bin/python3.6 -rl ./`
  cd ${venv_path}
}

function check_chameleon(){
  if [ -f $chameleon_path ];then
      version_str=$($chameleon_path --version)
  else
      echo "$chameleon_path not exist.." >> $install_log
  fi
}

function install_whl(){
  rm -rf $venv_path/venv
  python3 -m venv venv && $venv_path/venv/bin/pip3 install $venv_path/chameleon-$version-py3-none-any.whl 2>&1 >> $install_log
}


function main(){
  run_build
  check_chameleon
  if [ "$version_str" != "$chameleon_version" ]
  then
    echo "Check version failed, try to install whl package." >> $install_log
    install_whl
    check_chameleon
    if [ "$version_str" != "$chameleon_version" ]
    then
      echo "Check version failed." >> $install_log
    else
      echo "Check version success." >> $install_log
    fi
  else
    echo "Check version success." >> $install_log
  fi
}

main $@
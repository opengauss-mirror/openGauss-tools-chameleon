venv_path=$(pwd)
chameleon_path=${venv_path}/venv/bin/chameleon
version=6.0.0
chameleon_version="chameleon "$version
version_str=''
install_log=install.log
echo $venv_path

function run_build(){
  sh build.sh 2>&1 > build_chameleon.log
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
  python3 -m venv venv && $venv_path/venv/bin/pip3 install $venv_path/chameleon-$version-py3-none-any.whl 2>&1 > install_chameleon.log
}


function main(){
  check_chameleon
  if [ "$version_str" == "$chameleon_version" ]
  then
    echo "chameleon installed..." >> $install_log
    exit 0
  fi
  run_build
  check_chameleon
  if [ "$version_str" != "$chameleon_version" ]
  then
    echo "Check version failed, please check build_chameleon.log,try to install whl package." >> $install_log
    install_whl
    check_chameleon
    if [ "$version_str" != "$chameleon_version" ]
    then
      echo "Check version failed, please check install_chameleon.log." >> $install_log
    else
      echo "Check version success." >> $install_log
    fi
  else
    echo "Check version success." >> $install_log
  fi
}

main $@
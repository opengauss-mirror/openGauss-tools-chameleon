venv_path=$(pwd)
chameleon_path=${venv_path}/venv/bin/chameleon
version=5.0.0
chameleon_version="chameleon "$version
version_str=''
echo $venv_path

function run_build(){
  sh build.sh 2>&1 > build_chameleon.log
}

function check_chameleon(){
  version_str=$($chameleon_path --version)
}

function install_whl(){
  rm -rf $venv_path/venv
  rm -rf $venv_path/python3.6.8
  python3 -m venv venv && $venv_path/venv/bin/pip3 install $venv_path/chameleon-$version-py3-none-any.whl 2>&1 > install_chameleon.log
}


function main(){
  run_build
  check_chameleon
  if [ "$version_str" != "$chameleon_version" ]
  then
    echo "Check version failed, please check build_chameleon.log,try to install whl package."
    install_whl
    check_chameleon
    if [ "$version_str" != "$chameleon_version" ]
    then
      echo "Check version failed, please check install_chameleon.log." 
    else
      echo "Check version success."
    fi
  else
    echo "Check version success."
  fi
}

main $@

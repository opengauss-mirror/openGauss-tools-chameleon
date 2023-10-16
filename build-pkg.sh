#显示当前路径
version=5.1.1
path=`pwd`
echo $path
package_name=chameleon-$version
mkdir $package_name
package_path=$path/$package_name

#下载安装python,并自定义安装目录
python_path=$path/Python-3.6.8
if [ -f "$python_path" ]; then
echo "Python-3.6.8 has exists."
else
wget https://www.python.org/ftp/python/3.6.8/Python-3.6.8.tgz
tar -xvf Python-3.6.8.tgz
cd Python-3.6.8
./configure --prefix=$package_path/python3.6.8
make && make install
echo "install python 3.6.8 finished"
cd ../
fi

#安装chameleon
wget https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/chameleon-$version-py3-none-any.whl
cd $package_path
python3.6.8/bin/python3.6 -m venv venv
source venv/bin/activate
cd venv/bin
./pip3 install $path/chameleon-$version-py3-none-any.whl
echo "install chameleon finish"

#删除软链接
rm python3.6

#删除虚拟环境指定的python路径
sed -i "1s#$package_path##" `grep $package_path -rl ./`

#生成执行脚本
cd $package_path
echo "
current_path=\`pwd\`
echo \"
LD_LIBRARY_PATH=\`pwd\`:\\\$LD_LIBRARY_PATH
export LD_LIBRARY_PATH
\" >> ~/.bashrc
#解压后的操作
cd venv/bin
#创建软链接
ln -s \$current_path/python3.6.8/bin/python3.6 python3.6
#修改虚拟环境实际的python路径
sed -i \"1s#/venv/bin/python3.6#\$current_path/venv/bin/python3.6#\" \`grep /venv/bin/python3.6 -rl ./\`
" >> build.sh

#打包
cd $path
cp -rf ./install.sh $package_path
tar zcvf chameleon-$version.tar.gz $package_name

#删除临时文件
rm -rf chameleon-$version-py3-none-any.whl*

#检测chameleon是否安装成功
cd $package_path
sh build.sh
chameleontestversion=`$path/$package_name/venv/bin/chameleon --version`
if [ "$chameleontestversion" = "chameleon $version" ]; then
echo "Chameleon $version install success."
else
echo "Chameleon $version install fail."
fi

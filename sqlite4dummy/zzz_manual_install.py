##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL


Compatibility
-------------
    Python2: Yes
    Python3: Yes
    
    
Prerequisites
-------------
    None
    
    
Introduction(介绍)
------------------
[ENG]Put this script in your package directory, for example: mypackage\zzz_manual_install.py
And run this script as the main. Then your package is automatically installed and replace the
old one for all python version on your WINDOWS computer.
    This is called, dummy install. But you still have to make a setup.py and build the distribution
    by yourself. Read the following instruction:
    
        For python3:
            https://docs.python.org/2/distutils/setupscript.html
            https://docs.python.org/2/distutils/builtdist.html
        
        For python2:
            https://docs.python.org/3.3/distutils/setupscript.html
            https://docs.python.org/3.3/distutils/builtdist.html

    Warning: with python2, this script cannot use with non-ascil directory path
    
[CHN]本脚用于傻瓜一键安装用户自己写的扩展包, 纯python实现。

例如你有一个扩展包叫 mypackage, 文件目录形如: C:\project\mypackage
则只需要把该脚本拷贝到 mypackage 目录下: C:\project\mypackage\zzz_manual_install.py
然后将本脚本以主脚本运行。则会把package文件中所有的 .pyc 文件清除后, 安装你所有
已安装的Python版本下。例如你安装了Python27和Python33, 那么改脚本就会创建以下目录:

    C:\Python27\Lib\site-packages\mypackage
    C:\Python33\Lib\site-packages\mypackage
    
然后你就可以用 import mypackage 调用你写的库了。

这一功能在调试阶段非常方便, 但最终发布时还是要通过写setup.py文件来安装你的package。

注: 项目目录在python2中不允许有中文路径
"""

from __future__ import print_function, unicode_literals

if __name__ == "__main__":
    def install():
        import os, shutil
        
        _ROOT = os.getcwd()
        _PACKAGE_NAME = os.path.basename(_ROOT)
        
        print("Installing [%s] to all python version..." % _PACKAGE_NAME)
        # find all Python release installed on this windows computer
        installed_python_version = list()
        for root, folder_list, _ in os.walk(r"C:\\"):
            for folder in folder_list:
                if folder.startswith("Python"):
                    if os.path.exists(os.path.join(root, folder, "pythonw.exe")):
                        installed_python_version.append(folder)
            break
        print("\tYou have installed: {0}".format(", ".join(installed_python_version)))
        
        # remove __pycache__ folder and *.pyc file
        print("\tRemoving *.pyc file ...")
        pyc_folder_list = list()
        for root, folder_list, _ in os.walk(_ROOT):
            if os.path.basename(root) == "__pycache__":
                pyc_folder_list.append(root)
        
        for folder in pyc_folder_list:
            shutil.rmtree(folder)
        print("\t\tall *.pyc file has been removed.")
        
        # install this package to all python version
        for py_root in installed_python_version:
            dst = os.path.join(r"C:\\", py_root, r"Lib\site-packages", _PACKAGE_NAME)
            try:
                shutil.rmtree(dst)
            except:
                pass
            print("\tRemoved %s." % dst)
            shutil.copytree(_ROOT, dst)
            print("\tInstalled %s." % dst)
        
        print("Complete!")
        
    install()
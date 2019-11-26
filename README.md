# datasync
基于阿里DataX数据同步框架的二次开发。本项目基于SpringBoot框架，采用Java语言开发,Gradle构建。

# Features
本项目目前仅支持从sqlserver到mysql数据同步，但与其他数据库开发原理一致，可基于本项目进行其他数据库同步的开发。具体支持同步的数据库列表，可参考阿里Datax:
https://github.com/alibaba/DataX.

# Quick Start
本项目将依赖类库与依赖配置文件，分离到打包后jar文件之外，分别置于lib和configs文件夹中,jar包运行请使用命令：java -Xbootclasspath/a:./configs -jar datasync-1.0-SNAPSHOT.jar
指定配置文件运行

# Contact me
email：mtzq_peng@163.com

# License
GNU GENERAL PUBLIC LICENSE
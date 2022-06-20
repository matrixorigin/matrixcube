### 混沌测试的运行方法

#### 针对 matrixcube 的测试

程序在 matrixone 仓库的 pkg/chaostesting/testcube 目录下

使用 `go build` 命令编译，最低编译器版本是 1.18，或者参照此页面使用 tip 版本：https://pkg.go.dev/golang.org/dl/gotip

编译后，可以执行以下命令

`./testcube run`

这个命令会持续生成随机测试配置，并执行

`./testcube run testdata/02f05c35-8b23-43d6-8b1d-62b2407c63e8.config.xml`

这个命令会执行指定配置文件的测试

##### 测试结果

配置及结果的文件，会存入 testdata 目录

每个配置有唯一的 uuid，生成的文件以这个 uuid 为文件名的前缀

不同的后缀代表不同的文件类型，包括：

* .config.xml 测试配置文件
* .cube.log 各节点的 log
* .porcupine.log porcupine 测试的 operation log
* .porcupine.html porcupine 测试的 operation log 的可视化 html

##### 临时目录

测试程序使用系统临时目录，作为各个节点的数据目录

这些目录以 testcube- 为前缀

测试如果正常结束，会清理用到的数据目录

但测试可能非正常结束，临时目录可能没有清理，可以手工删除

##### 内部配置项

`script.py` 文件包含一些可以配置的参数

参数更改后无需重新编译程序

大致上不需要改动这些参数，有些选项只在特定平台有用，有些参数组合并不能工作


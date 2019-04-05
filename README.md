Octopus: A distributed web spider based on AKKA+Scala
======================

网络爬虫是进行科研数据分析、商业数据处理等活动的重要基础工具，本人基于之前的研究成果和实践经验，采用目前较新的Scala+Akka，设计实现一个高性能的分布式网络爬虫。目标如下：

1. 对于普通的使用人员，可以直接快速运行，并获取指定关键词的相关文章，保存到数据库之中。
2. 可以支持定制开发，并且尽可能降低定制的复杂度
3. 爬虫既可以单机运行，也可以分布式运行
4. 保证较低的系统负荷和较高的运行效率
5. 能够支持IP代理功能
6. 能够对指定的目标主机进行限速，避免对特定主机因网络爬虫造成过高的负担
7. 能够集成常规的正文自动抽取（正文抽取算法尚未开源）、关键词抽取、摘要等功能
8. 能够处理Javascript动态生成的链接
9. 能够实现网页截屏功能，把网页保存成图片和PDF格式，方便长期保存（如历史档案馆、网页归档）
10. 能够支持图片的采集和处理



## Compile & Run
### Install Mysql
```$sql
CREATE SCHEMA `octopus` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin ;

CREATE TABLE `epaper_article` (
  `id` varchar(254) COLLATE utf8_bin NOT NULL,
  `url` varchar(250) COLLATE utf8_bin NOT NULL,
  `title` varchar(250) COLLATE utf8_bin NOT NULL,
  `subtitle` varchar(250) COLLATE utf8_bin NOT NULL,
  `author` varchar(250) COLLATE utf8_bin NOT NULL,
  `pub_date` varchar(50) COLLATE utf8_bin NOT NULL,
  `media` varchar(50) COLLATE utf8_bin NOT NULL,
  `page` varchar(50) COLLATE utf8_bin NOT NULL,
  `rank` int(11) NOT NULL,
  `text` mediumtext COLLATE utf8_bin NOT NULL,
  `html` mediumtext COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

```

### Compile

```bash
git clone https://github.com/iamxiatian/octopus_spider.git octopus
cd octopus
sbt stage
```
### Run
```bash
cd target/universal/stage
bin/repo --create # 创建数据库表，否则采集结果无法入库，（TODO）
bin/task --inject # (TODO)
bin/spider --master --fetcher
```

## 核心对象

FetchItem: 表示一个被抓取的条目，一个FetchItem除了value等基本信息之外，还包含
两个重要信息，即所属抓取类型和所属任务，所属类型确定了链接采集后的处理方法，例如，是文章链接，
还是导航链接，还是电子报的栏目或者文章；而所属的任务则描述了采集任务的一些参数规定，例如，针对某新闻站点的采集任务，要求每隔10分钟扫描一次首页，二级页面1天搜索一次等要求。

## 目标

1. 采集数据的存储支持分布式逻辑，先在每个Fetcher内部保存，并通过Akka Stream发送到StoreMasterActor
集中保存。

2. 代理管理：尽量把一个代理发送到一个Fetcher上，并能够自动移除无效代理

## 更改历史

- 保存处理，由StoreActor异步保存改为同步保存，以便当保存失败时可以重复抓取
- 电子报采集的文章增加副标题subTitle
- 可以编译运行并高效抓取电子报，版本升级到2.0
- 完成了电子报采集逻辑的处理，并成功加入了一个示例
- 引入slick处理关系性数据库，采集结果目前暂时存入MySQL，方便观察调试
- MyConf中增加了可以增强Config功能的类
- 增强url transformer的功能，可以根据正则表达式，对源码中获取的链接进行变换，发现搜索引擎列表页面链接中的真实目标地址
- 加入统一的日志特质Logging，方便统一管理日志

## Thanks

项目用到了许多开源模块，为方便编译和调整逻辑，把部分开源软件的代码集成到了本项目之中，并调整了代码的包名称，对此表示谢意。

致谢列表：

1. https://akka.io/
2. https://circe.github.io/circe/
3. http://slick.lightbend.com/
4. http://irm.ruc.edu.cn/

...

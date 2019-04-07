# 安装配置

## 数据库配置
数据库的名称可以在conf/my.conf文件中修改，默认为octopus, 以utf-8编码创建数据库后，再创建表

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

修改conf/my.conf中的store.db.mysql信息，与实际环境保持一致

## 初始化任务
bin/fetch-task，就会创建任务，只需要第一次运行的时候执行一次即可

## 抓取

注意，现在两个参数需要一起运行，不能先启动master，再启动fetcher

bin/spider --fetcher --master


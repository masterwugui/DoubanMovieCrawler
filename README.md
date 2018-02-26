# DoubanMovieCrawler
基于webmagic框架的轻量级爬虫。

因为没有找到合适的代理地址,为了防止被豆瓣屏蔽未使用多线程。

框架部分的代码并未上传，上传类的说明如下：

1.CrawlerDoubanMovieNew.java： 爬虫类，以豆瓣电影分类的地址作为入口进行广度优先的爬虫，从每一部电影的相关推荐中获取新的需要爬取的地址，设置布隆过滤器来对地址进行去重（guava类库中的BloomFilter实现）；页面解析的process()方法只适用于绝大多数电影详情页，有部分冷门的、过于古老的电影豆瓣电影还是使用了以前的元素（原因未知）导致无法正确的爬取；

2.DoubanModelPipeline.java：数据管道类，此类自定义爬取到的数据的存储方式，本项目将所有爬取的电影数据存储到数据库中，采用mysql数据库，数据库版本为5.7；

3.MovieModel.java：数据model类。

共计爬取了豆瓣电影网影视剧相关数据总计约8万多条，数据截图如下：

![image](https://github.com/masterwugui/DoubanMovieCrawler/blob/master/images/%E6%95%B0%E6%8D%AE%E5%BA%93%E5%AD%98%E5%82%A8%E6%95%B0%E6%8D%AE.png)


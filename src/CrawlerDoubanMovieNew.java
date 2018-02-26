package cetc.com.webmagic.twitch;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jsoup.helper.StringUtil;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.DoubanModelPipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.scheduler.BloomFilterDuplicateRemover;
import us.codecraft.webmagic.scheduler.FileCacheQueueScheduler;
import us.codecraft.webmagic.selector.Html;

/**
 * 
 * @author lujunzizi_sleep
 * @see 重新写了一个 这次需要把raw数据完全存到库里 考虑从分类着手
 */
public class CrawlerDoubanMovieNew implements PageProcessor {
	public static Site site = Site
			.me()
			.setRetryTimes(1)
			.setCycleRetryTimes(2)
			.setDomain("movie.douban.com")
			.setSleepTime(3000)
			.setTimeOut(3000)
			.setUserAgent(
					"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.59 Safari/537.36")
			.addCookie("gr_user_id", "e9674f00-4d95-4c48-bb61-70b7ed19c811")
			.addCookie("ll", "118159")
			.addCookie(
					"viewed",
					"25885921_6709809_6811366_10750155_6709783_1089243_1016107_3224524_25965995_3735649")
			.addCookie("bid", "v205EAKxUho")
			.addCookie("_vwo_uuid_v2",
					"9736679276E1838D988AE94ADBE05679|252f3512dd984088c29c6b5bea8804fa")
			.addCookie("ps", "y")
			.addCookie(
					"_pk_ref.100001.8cb4",
					"%5B%22%22%2C%22%22%2C1488798908%2C%22https%3A%2F%2Faccounts.douban.com%2Flogin%3Falias%3D15751866073%26redir%3Dhttps%253A%252F%252Fwww.douban.com%252Fgroup%252Fsearch%253Fstart%253D0%2526cat%253D1019%2526sort%253Drelevance%2526q%253D%2525E8%2525AF%2525B7%2525E4%2525B8%25258D%2525E8%2525A6%252581%2525E5%2525AE%2525B3%2525E7%2525BE%25259E%26source%3DNone%26error%3D1027%22%5D")
			.addCookie("__utmt", "1")
			.addCookie("ap", "1")
			.addCookie("ue", "452756565@qq.com")
			.addCookie("dbcl2", "45483389:8BjNYkc8Il8")
			.addCookie("ck", "ZdMH")
			.addCookie("push_noty_num", "0")
			.addCookie("push_doumail_num", "0")
			.addCookie("_pk_id.100001.8cb4",
					"47b2976ee4f3a82f.1427790671.69.1488800072.1488471613.")
			.addCookie("_pk_ses.100001.8cb4", "*")
			.addCookie("__utma",
					"30149280.1333754830.1427790902.1488467949.1488798909.74")
			.addCookie("__utmb", "30149280.56.5.1488800072003")
			.addCookie("__utmc", "30149280")
			.addCookie(
					"__utmz",
					"30149280.1488467949.73.24.utmcsr=accounts.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/login")
			.addCookie("__utmv", "30149280.4548")
			.addHeader("Accept",
					"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
			.addHeader("Accept-Encoding", "gzip,deflate,sdch")
			.addHeader("Accept-Language", "zh-CN,zh;q=0.8")
			.addHeader("Connection", "keep-alive")
			.addHeader("Cache-Control", "max-age=0");

	private static final String URL_DOUBAN_TYPES_TEMPLATE = "https://movie.douban.com/tag/?view=type";

	private static final String URL_DOUBAN_TYPE_LIST_TEMPLATE = "https://movie\\.douban\\.com/tag/\\w+";

	private static final String URL_FILM_INFO_TEMPLATE = "https://movie\\.douban\\.com/subject/\\d+/";

	private static final String appendix = "?from=subject-page";

	public static void main(String[] args) {
		Spider spider = Spider.create(new CrawlerDoubanMovieNew());
		spider.addRequest(new Request(URL_DOUBAN_TYPES_TEMPLATE));
		spider.addPipeline(new DoubanModelPipeline());
		spider.setScheduler(new FileCacheQueueScheduler(
				"D://webmagic//fileCache")
				.setDuplicateRemover(new BloomFilterDuplicateRemover(1000000)));
		spider.run();
	}

	@Override
	public void process(Page page) {
		String pageUrl = page.getUrl().toString();
		Html parseHtml = page.getHtml();
		// 标签页
		if (page.getUrl().toString().equals(URL_DOUBAN_TYPES_TEMPLATE)) {
			List<String> TagList = parseHtml.xpath("//div[@class=\"article\"]")
					.links().all();
			page.addTargetRequests(TagList);
			page.setSkip(true);
			// 列表页
		} else if (pageUrl.contains("tag") && !pageUrl.contains("cloud")) {
			List<String> PagList = parseHtml
					.xpath("//div[@class=\"paginator\"]").links().all();
			page.addTargetRequests(PagList);

			List<String> filmList = parseHtml.xpath("//div[@class=\"pl2\"]")
					.links().all();
			page.addTargetRequests(filmList);
			page.setSkip(true);
			// 电影页
		} else if (page.getUrl().regex(URL_FILM_INFO_TEMPLATE).match()) {
			// 将所有相关电影加入urlList
			List<String> filmList = parseHtml
					.xpath("//div[@class=\"recommendations-bd\"]").links()
					.all();
			List<String> newFilmList = new ArrayList<String>();
			for (String filmUrl : filmList) {
				String newFilmURL = filmUrl.replace(appendix, "");
				newFilmList.add(newFilmURL);
			}
			page.addTargetRequests(newFilmList);
			// 爬取电影详情
			// 如果没有分数则不进行记录
			// 豆瓣评分
			String point = parseHtml.xpath(
					"//strong[@class='ll rating_num']/tidyText()").toString();
			String url = page.getUrl().toString();
			String targetId = url.substring(url.indexOf("subject") + 8)
					.replace("/", "");
			page.putField("targetId", targetId);
			if (StringUtil.isBlank(point)) {
				// System.out.println(page.getUrl());
				page.putField("point", "0");
				// 评分人数
				page.putField("pointNum", "0");
				// 5星
				page.putField("5Star", "0");
				// 4星
				page.putField("4Star", "0");
				// 3星
				page.putField("3Star", "0");
				// 2星
				page.putField("2Star", "0");
				// 1星
				page.putField("1Star", "0");
			} else {
				page.putField("point", point);
				// 评分人数
				String pointNum = parseHtml.xpath(
						"//span[@property='v:votes']/tidyText()").toString();
				page.putField("pointNum", pointNum);
				// 5星
				List<String> starNums = parseHtml
						.xpath("//div[@class='ratings-on-weight']//span[@class='rating_per']/text()")
						.all();
				page.putField(
						"5Star",
						starNums.get(0).substring(0,
								starNums.get(0).indexOf("%")));
				// 4星
				page.putField(
						"4Star",
						starNums.get(1).substring(0,
								starNums.get(1).indexOf("%")));
				// 3星
				page.putField(
						"3Star",
						starNums.get(2).substring(0,
								starNums.get(2).indexOf("%")));
				// 2星
				page.putField(
						"2Star",
						starNums.get(3).substring(0,
								starNums.get(3).indexOf("%")));
				// 1星
				page.putField(
						"1Star",
						starNums.get(4).substring(0,
								starNums.get(4).indexOf("%")));
			}
			// 电影标题
			String title = parseHtml.xpath(
					"//h1/span[@property='v:itemreviewed']/tidyText()")
					.toString();
			page.putField("title", title);
			if (StringUtil.isBlank(title)) {
				System.out.println(page.getUrl());
			}
			// 电影年份
			String year = parseHtml
					.xpath("//h1/span[@class='year']/tidyText()").toString();
			page.putField("year", StringUtil.isBlank(year) ? "0" : year
					.replace("(", "").replace(")", ""));
			// 导演
			String director = StringUtils
					.join(parseHtml
							.xpath("//div[@id='info']//a[@rel='v:directedBy']/text()")
							.all().toArray(), "/");
			page.putField("director", director);
			// 编剧
			String hasScreenwriter = parseHtml.xpath(
					"//div[@id='info']/span[2]/span[@class='pl']/text()")
					.toString();

			String screenwriter = StringUtil.isBlank(hasScreenwriter) ? ""
					: hasScreenwriter.equals("编剧") ? parseHtml
							.xpath("//div[@id='info']/span[2]/span[@class='attrs']/allText()")
							.toString()
							: "";
			page.putField("screenwriter", screenwriter);
			// 主演
			boolean hasActors = StringUtil.isBlank(parseHtml.xpath(
					"//div[@id='info']//a[@rel='v:starring']/text()")
					.toString());
			if (!hasActors) {
				String actors = StringUtils
						.join(parseHtml
								.xpath("//div[@id='info']//a[@rel='v:starring']/text()")
								.all().toArray(), "/");
				page.putField("actors", actors);
			} else {
				page.putField("actors", "");
			}
			// 类型 重写 完成
			boolean hasTypes = StringUtil.isBlank(parseHtml.xpath(
					"//div[@id='info']//span[@property='v:genre']/text()")
					.toString());
			if (!hasTypes) {
				String types = StringUtils
						.join(parseHtml
								.xpath("//div[@id='info']//span[@property='v:genre']/text()")
								.all().toArray(), "/");
				page.putField("types", types);
			} else {
				page.putField("types", "");
			}

			// 制片国家/地区
			String filmInfoStr = parseHtml.xpath("//div[@id='info']/text()")
					.toString();
			String[] filmInfo = { "", "" };
			filmInfo = StringUtil.isBlank(filmInfoStr) ? filmInfo : filmInfoStr
					.replaceAll(" / ", "").trim().split("  ");
			page.putField("country", filmInfo.length > 0 ? filmInfo[0] : "");
			// 语言 上映日期 可能为空 已修改
			if (filmInfo.length > 1)
				page.putField("language", filmInfo.length > 1 ? filmInfo[1]
						: "");

			// 上映日期 可能为空
			String date = parseHtml
					.xpath("//div[@id='info']/span[@property='v:initialReleaseDate']/text()")
					.toString();
			page.putField("date", StringUtil.isBlank(date) ? "" : date);

			// 片长 可能为空
			String length = parseHtml.xpath(
					"//div[@id='info']/span[@property='v:runtime']/text()")
					.toString();
			page.putField("length", StringUtil.isBlank(length) ? "" : length);

			// IMDB
			String imdb = parseHtml.xpath(
					"//div[@id='info']/a[@rel='nofollow']/text()").toString();
			page.putField("imdb", StringUtil.isBlank(imdb) ? "" : imdb);
		}
	}

	@Override
	public Site getSite() {
		return site;
	}

}

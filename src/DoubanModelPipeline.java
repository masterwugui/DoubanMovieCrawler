package us.codecraft.webmagic.pipeline;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.model.MovieModel;

public class DoubanModelPipeline implements Pipeline {

	public static void main(String[] args) {
	}

	private static void InsertMovie(MovieModel model) {
		Connection conn = getConn();
		String sql = "insert into movie_all (point,point_num,5Star,4Star,3Star,2Star,1Star"
				+ ",title,director,screenwriter,actors,types,country,language,date,length"
				+ ",imdb,target_id,year) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pstmt;
		try {
			pstmt = (PreparedStatement) conn.prepareStatement(sql);
			pstmt.setDouble(1, model.getPoint());
			pstmt.setInt(2, model.getPoint_num());
			pstmt.setDouble(3, model.getStar5());
			pstmt.setDouble(4, model.getStar4());
			pstmt.setDouble(5, model.getStar3());
			pstmt.setDouble(6, model.getStar2());
			pstmt.setDouble(7, model.getStar1());
			pstmt.setString(8, model.getTitle());
			pstmt.setString(9, model.getDirector());
			pstmt.setString(10, model.getScreenwriter());
			pstmt.setString(11, model.getActors());
			pstmt.setString(12, model.getTypes());
			pstmt.setString(13, model.getCountry());
			pstmt.setString(14, model.getLanguage());
			pstmt.setString(15, model.getDate());
			pstmt.setString(16, model.getLength());
			pstmt.setString(17, model.getImdb());
			pstmt.setString(18, model.getTargetId());
			pstmt.setString(19, model.getYear());
			int i = pstmt.executeUpdate();
			pstmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static Connection getConn() {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/douban?useUnicode=true&characterEncoding=utf8";
		String username = "root";
		String password = "";
		Connection conn = null;
		try {
			Class.forName(driver); // classLoader,加载对应驱动
			conn = (Connection) DriverManager.getConnection(url, username,
					password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	@Override
	public void process(ResultItems resultItems, Task task) {
		MovieModel movie = new MovieModel();
		for (Map.Entry<String, Object> entry : resultItems.getAll().entrySet()) {
			switch (entry.getKey()) {
			case "point":
				movie.setPoint(Double.parseDouble((String) entry.getValue()));
				break;
			case "pointNum":
				movie.setPoint_num(Integer.parseInt((String) entry.getValue()));
				break;
			case "5Star":
				movie.setStar5(Double.parseDouble((String) entry.getValue()));
				break;
			case "4Star":
				movie.setStar4(Double.parseDouble((String) entry.getValue()));
				break;
			case "3Star":
				movie.setStar3(Double.parseDouble((String) entry.getValue()));
				break;
			case "2Star":
				movie.setStar2(Double.parseDouble((String) entry.getValue()));
				break;
			case "1Star":
				movie.setStar1(Double.parseDouble((String) entry.getValue()));
				break;
			case "title":
				movie.setTitle((String) entry.getValue());
				break;
			case "director":
				movie.setDirector((String) entry.getValue());
				break;
			case "screenwriter":
				movie.setScreenwriter((String) entry.getValue());
				break;
			case "actors":
				movie.setActors((String) entry.getValue());
				break;
			case "types":
				movie.setTypes((String) entry.getValue());
				break;
			case "country":
				movie.setCountry((String) entry.getValue());
				break;
			case "language":
				movie.setLanguage((String) entry.getValue());
				break;
			case "date":
				movie.setDate((String) entry.getValue());
				break;
			case "length":
				movie.setLength((String) entry.getValue());
				break;
			case "imdb":
				movie.setImdb((String) entry.getValue());
				break;
			case "targetId":
				movie.setTargetId((String) entry.getValue());
				break;
			case "year":
				movie.setYear((String)entry.getValue());
				break;
			}
		}
		InsertMovie(movie);
	}
}

package utils
import java.sql.{Connection, DriverManager, PreparedStatement}
/**
  * Created by Michael on 2016/11/7.
  */
object DataUtil {

      def toMySQL(name: String,click:Int) = {
        var conn: Connection = null
        var ps: PreparedStatement = null
        //val sql = "update click_statistic set name=?,click=?"
        //val sql = "insert into click_statistic values(?,?)"
        val sql = "REPLACE INTO click_statistic(NAME, click) VALUES(?, ?)"
        try {
          Class.forName("com.mysql.jdbc.Driver");
          conn = DriverManager.getConnection("jdbc:mysql://192.168.20.126:3306/test", "root", "root")

          ps = conn.prepareStatement(sql)
          ps.setString(1, name)
          ps.setInt(2, click)
          ps.executeUpdate()
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }


      //日志清洗  def convert_insert(s: String,fields:Array[String]): Array[String] = {
      def convert_insert(s: String,fields:Array[String]): String = {
        var i:Int=0
        var index:Int=0
        var len=fields.length
        var ret_arr:Array[String]=new Array[String](len)
        //列的数组初始化，值为空
        while (i < len){
          ret_arr(i)="''"
          i=i+1
        }
        val line = s.trim().split("\001")
        i=0
        //将日志中的值循环赋给列
        while (i < line.length){
          val kv = line(i).trim().split("=")
          if (kv.length == 2){
            index=fields.indexOf(kv(0).trim())
            if (index>=0 && index<len){
              ret_arr(index)=kv(1)
            }
          }
          i=i+1
        }
        //var str1 = ret_arr.mkString("('","','","')")
        var str1 = ret_arr.mkString(",")
        return str1
      }
}

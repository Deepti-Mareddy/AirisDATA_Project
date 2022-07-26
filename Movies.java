// ORM class for table 'Movies'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Fri Jul 22 10:03:10 IST 2022
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Movies extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public Movies with_id(Integer id) {
    this.id = id;
    return this;
  }
  private String name;
  public String get_name() {
    return name;
  }
  public void set_name(String name) {
    this.name = name;
  }
  public Movies with_name(String name) {
    this.name = name;
    return this;
  }
  private String rating;
  public String get_rating() {
    return rating;
  }
  public void set_rating(String rating) {
    this.rating = rating;
  }
  public Movies with_rating(String rating) {
    this.rating = rating;
    return this;
  }
  private String genre;
  public String get_genre() {
    return genre;
  }
  public void set_genre(String genre) {
    this.genre = genre;
  }
  public Movies with_genre(String genre) {
    this.genre = genre;
    return this;
  }
  private Integer year;
  public Integer get_year() {
    return year;
  }
  public void set_year(Integer year) {
    this.year = year;
  }
  public Movies with_year(Integer year) {
    this.year = year;
    return this;
  }
  private java.sql.Date released_date;
  public java.sql.Date get_released_date() {
    return released_date;
  }
  public void set_released_date(java.sql.Date released_date) {
    this.released_date = released_date;
  }
  public Movies with_released_date(java.sql.Date released_date) {
    this.released_date = released_date;
    return this;
  }
  private Float score;
  public Float get_score() {
    return score;
  }
  public void set_score(Float score) {
    this.score = score;
  }
  public Movies with_score(Float score) {
    this.score = score;
    return this;
  }
  private Integer votes;
  public Integer get_votes() {
    return votes;
  }
  public void set_votes(Integer votes) {
    this.votes = votes;
  }
  public Movies with_votes(Integer votes) {
    this.votes = votes;
    return this;
  }
  private String director;
  public String get_director() {
    return director;
  }
  public void set_director(String director) {
    this.director = director;
  }
  public Movies with_director(String director) {
    this.director = director;
    return this;
  }
  private String writer;
  public String get_writer() {
    return writer;
  }
  public void set_writer(String writer) {
    this.writer = writer;
  }
  public Movies with_writer(String writer) {
    this.writer = writer;
    return this;
  }
  private String star;
  public String get_star() {
    return star;
  }
  public void set_star(String star) {
    this.star = star;
  }
  public Movies with_star(String star) {
    this.star = star;
    return this;
  }
  private String country;
  public String get_country() {
    return country;
  }
  public void set_country(String country) {
    this.country = country;
  }
  public Movies with_country(String country) {
    this.country = country;
    return this;
  }
  private Long budget;
  public Long get_budget() {
    return budget;
  }
  public void set_budget(Long budget) {
    this.budget = budget;
  }
  public Movies with_budget(Long budget) {
    this.budget = budget;
    return this;
  }
  private Long gross;
  public Long get_gross() {
    return gross;
  }
  public void set_gross(Long gross) {
    this.gross = gross;
  }
  public Movies with_gross(Long gross) {
    this.gross = gross;
    return this;
  }
  private String company;
  public String get_company() {
    return company;
  }
  public void set_company(String company) {
    this.company = company;
  }
  public Movies with_company(String company) {
    this.company = company;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Movies)) {
      return false;
    }
    Movies that = (Movies) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.name == null ? that.name == null : this.name.equals(that.name));
    equal = equal && (this.rating == null ? that.rating == null : this.rating.equals(that.rating));
    equal = equal && (this.genre == null ? that.genre == null : this.genre.equals(that.genre));
    equal = equal && (this.year == null ? that.year == null : this.year.equals(that.year));
    equal = equal && (this.released_date == null ? that.released_date == null : this.released_date.equals(that.released_date));
    equal = equal && (this.score == null ? that.score == null : this.score.equals(that.score));
    equal = equal && (this.votes == null ? that.votes == null : this.votes.equals(that.votes));
    equal = equal && (this.director == null ? that.director == null : this.director.equals(that.director));
    equal = equal && (this.writer == null ? that.writer == null : this.writer.equals(that.writer));
    equal = equal && (this.star == null ? that.star == null : this.star.equals(that.star));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.budget == null ? that.budget == null : this.budget.equals(that.budget));
    equal = equal && (this.gross == null ? that.gross == null : this.gross.equals(that.gross));
    equal = equal && (this.company == null ? that.company == null : this.company.equals(that.company));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Movies)) {
      return false;
    }
    Movies that = (Movies) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.name == null ? that.name == null : this.name.equals(that.name));
    equal = equal && (this.rating == null ? that.rating == null : this.rating.equals(that.rating));
    equal = equal && (this.genre == null ? that.genre == null : this.genre.equals(that.genre));
    equal = equal && (this.year == null ? that.year == null : this.year.equals(that.year));
    equal = equal && (this.released_date == null ? that.released_date == null : this.released_date.equals(that.released_date));
    equal = equal && (this.score == null ? that.score == null : this.score.equals(that.score));
    equal = equal && (this.votes == null ? that.votes == null : this.votes.equals(that.votes));
    equal = equal && (this.director == null ? that.director == null : this.director.equals(that.director));
    equal = equal && (this.writer == null ? that.writer == null : this.writer.equals(that.writer));
    equal = equal && (this.star == null ? that.star == null : this.star.equals(that.star));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.budget == null ? that.budget == null : this.budget.equals(that.budget));
    equal = equal && (this.gross == null ? that.gross == null : this.gross.equals(that.gross));
    equal = equal && (this.company == null ? that.company == null : this.company.equals(that.company));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.name = JdbcWritableBridge.readString(2, __dbResults);
    this.rating = JdbcWritableBridge.readString(3, __dbResults);
    this.genre = JdbcWritableBridge.readString(4, __dbResults);
    this.year = JdbcWritableBridge.readInteger(5, __dbResults);
    this.released_date = JdbcWritableBridge.readDate(6, __dbResults);
    this.score = JdbcWritableBridge.readFloat(7, __dbResults);
    this.votes = JdbcWritableBridge.readInteger(8, __dbResults);
    this.director = JdbcWritableBridge.readString(9, __dbResults);
    this.writer = JdbcWritableBridge.readString(10, __dbResults);
    this.star = JdbcWritableBridge.readString(11, __dbResults);
    this.country = JdbcWritableBridge.readString(12, __dbResults);
    this.budget = JdbcWritableBridge.readLong(13, __dbResults);
    this.gross = JdbcWritableBridge.readLong(14, __dbResults);
    this.company = JdbcWritableBridge.readString(15, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.name = JdbcWritableBridge.readString(2, __dbResults);
    this.rating = JdbcWritableBridge.readString(3, __dbResults);
    this.genre = JdbcWritableBridge.readString(4, __dbResults);
    this.year = JdbcWritableBridge.readInteger(5, __dbResults);
    this.released_date = JdbcWritableBridge.readDate(6, __dbResults);
    this.score = JdbcWritableBridge.readFloat(7, __dbResults);
    this.votes = JdbcWritableBridge.readInteger(8, __dbResults);
    this.director = JdbcWritableBridge.readString(9, __dbResults);
    this.writer = JdbcWritableBridge.readString(10, __dbResults);
    this.star = JdbcWritableBridge.readString(11, __dbResults);
    this.country = JdbcWritableBridge.readString(12, __dbResults);
    this.budget = JdbcWritableBridge.readLong(13, __dbResults);
    this.gross = JdbcWritableBridge.readLong(14, __dbResults);
    this.company = JdbcWritableBridge.readString(15, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(rating, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(genre, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(year, 5 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDate(released_date, 6 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeFloat(score, 7 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(votes, 8 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(director, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(writer, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(star, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(budget, 13 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(gross, 14 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(company, 15 + __off, 12, __dbStmt);
    return 15;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(rating, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(genre, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(year, 5 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDate(released_date, 6 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeFloat(score, 7 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(votes, 8 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(director, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(writer, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(star, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(budget, 13 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(gross, 14 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(company, 15 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.name = null;
    } else {
    this.name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.rating = null;
    } else {
    this.rating = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.genre = null;
    } else {
    this.genre = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.year = null;
    } else {
    this.year = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.released_date = null;
    } else {
    this.released_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.score = null;
    } else {
    this.score = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.votes = null;
    } else {
    this.votes = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.director = null;
    } else {
    this.director = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.writer = null;
    } else {
    this.writer = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.star = null;
    } else {
    this.star = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.country = null;
    } else {
    this.country = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.budget = null;
    } else {
    this.budget = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.gross = null;
    } else {
    this.gross = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.company = null;
    } else {
    this.company = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, name);
    }
    if (null == this.rating) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, rating);
    }
    if (null == this.genre) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, genre);
    }
    if (null == this.year) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.year);
    }
    if (null == this.released_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.released_date.getTime());
    }
    if (null == this.score) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.score);
    }
    if (null == this.votes) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.votes);
    }
    if (null == this.director) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, director);
    }
    if (null == this.writer) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, writer);
    }
    if (null == this.star) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, star);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.budget) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.budget);
    }
    if (null == this.gross) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.gross);
    }
    if (null == this.company) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, company);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, name);
    }
    if (null == this.rating) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, rating);
    }
    if (null == this.genre) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, genre);
    }
    if (null == this.year) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.year);
    }
    if (null == this.released_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.released_date.getTime());
    }
    if (null == this.score) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.score);
    }
    if (null == this.votes) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.votes);
    }
    if (null == this.director) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, director);
    }
    if (null == this.writer) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, writer);
    }
    if (null == this.star) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, star);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.budget) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.budget);
    }
    if (null == this.gross) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.gross);
    }
    if (null == this.company) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, company);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(name==null?"null":name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(rating==null?"null":rating, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(genre==null?"null":genre, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(year==null?"null":"" + year, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(released_date==null?"null":"" + released_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(score==null?"null":"" + score, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(votes==null?"null":"" + votes, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(director==null?"null":director, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(writer==null?"null":writer, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(star==null?"null":star, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(budget==null?"null":"" + budget, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(gross==null?"null":"" + gross, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(company==null?"null":company, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(name==null?"null":name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(rating==null?"null":rating, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(genre==null?"null":genre, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(year==null?"null":"" + year, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(released_date==null?"null":"" + released_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(score==null?"null":"" + score, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(votes==null?"null":"" + votes, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(director==null?"null":director, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(writer==null?"null":writer, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(star==null?"null":star, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(budget==null?"null":"" + budget, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(gross==null?"null":"" + gross, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(company==null?"null":company, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.name = null; } else {
      this.name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.rating = null; } else {
      this.rating = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.genre = null; } else {
      this.genre = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.year = null; } else {
      this.year = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.released_date = null; } else {
      this.released_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.score = null; } else {
      this.score = Float.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.votes = null; } else {
      this.votes = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.director = null; } else {
      this.director = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.writer = null; } else {
      this.writer = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.star = null; } else {
      this.star = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.budget = null; } else {
      this.budget = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.gross = null; } else {
      this.gross = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.company = null; } else {
      this.company = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.name = null; } else {
      this.name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.rating = null; } else {
      this.rating = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.genre = null; } else {
      this.genre = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.year = null; } else {
      this.year = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.released_date = null; } else {
      this.released_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.score = null; } else {
      this.score = Float.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.votes = null; } else {
      this.votes = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.director = null; } else {
      this.director = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.writer = null; } else {
      this.writer = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.star = null; } else {
      this.star = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.budget = null; } else {
      this.budget = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.gross = null; } else {
      this.gross = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.company = null; } else {
      this.company = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    Movies o = (Movies) super.clone();
    o.released_date = (o.released_date != null) ? (java.sql.Date) o.released_date.clone() : null;
    return o;
  }

  public void clone0(Movies o) throws CloneNotSupportedException {
    o.released_date = (o.released_date != null) ? (java.sql.Date) o.released_date.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("name", this.name);
    __sqoop$field_map.put("rating", this.rating);
    __sqoop$field_map.put("genre", this.genre);
    __sqoop$field_map.put("year", this.year);
    __sqoop$field_map.put("released_date", this.released_date);
    __sqoop$field_map.put("score", this.score);
    __sqoop$field_map.put("votes", this.votes);
    __sqoop$field_map.put("director", this.director);
    __sqoop$field_map.put("writer", this.writer);
    __sqoop$field_map.put("star", this.star);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("budget", this.budget);
    __sqoop$field_map.put("gross", this.gross);
    __sqoop$field_map.put("company", this.company);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("name", this.name);
    __sqoop$field_map.put("rating", this.rating);
    __sqoop$field_map.put("genre", this.genre);
    __sqoop$field_map.put("year", this.year);
    __sqoop$field_map.put("released_date", this.released_date);
    __sqoop$field_map.put("score", this.score);
    __sqoop$field_map.put("votes", this.votes);
    __sqoop$field_map.put("director", this.director);
    __sqoop$field_map.put("writer", this.writer);
    __sqoop$field_map.put("star", this.star);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("budget", this.budget);
    __sqoop$field_map.put("gross", this.gross);
    __sqoop$field_map.put("company", this.company);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
    }
    else    if ("name".equals(__fieldName)) {
      this.name = (String) __fieldVal;
    }
    else    if ("rating".equals(__fieldName)) {
      this.rating = (String) __fieldVal;
    }
    else    if ("genre".equals(__fieldName)) {
      this.genre = (String) __fieldVal;
    }
    else    if ("year".equals(__fieldName)) {
      this.year = (Integer) __fieldVal;
    }
    else    if ("released_date".equals(__fieldName)) {
      this.released_date = (java.sql.Date) __fieldVal;
    }
    else    if ("score".equals(__fieldName)) {
      this.score = (Float) __fieldVal;
    }
    else    if ("votes".equals(__fieldName)) {
      this.votes = (Integer) __fieldVal;
    }
    else    if ("director".equals(__fieldName)) {
      this.director = (String) __fieldVal;
    }
    else    if ("writer".equals(__fieldName)) {
      this.writer = (String) __fieldVal;
    }
    else    if ("star".equals(__fieldName)) {
      this.star = (String) __fieldVal;
    }
    else    if ("country".equals(__fieldName)) {
      this.country = (String) __fieldVal;
    }
    else    if ("budget".equals(__fieldName)) {
      this.budget = (Long) __fieldVal;
    }
    else    if ("gross".equals(__fieldName)) {
      this.gross = (Long) __fieldVal;
    }
    else    if ("company".equals(__fieldName)) {
      this.company = (String) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
      return true;
    }
    else    if ("name".equals(__fieldName)) {
      this.name = (String) __fieldVal;
      return true;
    }
    else    if ("rating".equals(__fieldName)) {
      this.rating = (String) __fieldVal;
      return true;
    }
    else    if ("genre".equals(__fieldName)) {
      this.genre = (String) __fieldVal;
      return true;
    }
    else    if ("year".equals(__fieldName)) {
      this.year = (Integer) __fieldVal;
      return true;
    }
    else    if ("released_date".equals(__fieldName)) {
      this.released_date = (java.sql.Date) __fieldVal;
      return true;
    }
    else    if ("score".equals(__fieldName)) {
      this.score = (Float) __fieldVal;
      return true;
    }
    else    if ("votes".equals(__fieldName)) {
      this.votes = (Integer) __fieldVal;
      return true;
    }
    else    if ("director".equals(__fieldName)) {
      this.director = (String) __fieldVal;
      return true;
    }
    else    if ("writer".equals(__fieldName)) {
      this.writer = (String) __fieldVal;
      return true;
    }
    else    if ("star".equals(__fieldName)) {
      this.star = (String) __fieldVal;
      return true;
    }
    else    if ("country".equals(__fieldName)) {
      this.country = (String) __fieldVal;
      return true;
    }
    else    if ("budget".equals(__fieldName)) {
      this.budget = (Long) __fieldVal;
      return true;
    }
    else    if ("gross".equals(__fieldName)) {
      this.gross = (Long) __fieldVal;
      return true;
    }
    else    if ("company".equals(__fieldName)) {
      this.company = (String) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}

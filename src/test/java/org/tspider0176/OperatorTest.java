package org.tspider0176;

import com.mongodb.MongoClient;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;
import sun.util.resources.cldr.ebu.CalendarData_ebu_KE;

import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class OperatorTest{
    // OrderByテストを行うための初期データをこの関数内で挿入する
    @Before
    public void initialize(){
        final Morphia morphia = new Morphia();

        // データベースごと初期化する
        MongoClient mongoClient = new MongoClient();
        mongoClient.dropDatabase("student");
        mongoClient.dropDatabase("music");

        // Student用の初期データ挿入
        final Datastore stu_datastore = morphia.createDatastore(new MongoClient(), "student");

        stu_datastore.save(new Student(1, "Tom"));
        stu_datastore.save(new Student(2, "Jack"));
        stu_datastore.save(new Student(3, "John"));
        stu_datastore.save(new Student(4, "Mike"));

        // betweenテストの為の初期データ挿入
        final Datastore mus_datastore = morphia.createDatastore(new MongoClient(), "music");

        final Date cal1 = new GregorianCalendar(1995, 4, 15).getTime();
        final Date cal2 = new GregorianCalendar(2000, 10, 3).getTime();
        final Date cal3 = new GregorianCalendar(2005, 6, 4).getTime();
        final Date cal4 = new GregorianCalendar(2010, 5, 29).getTime();

        mus_datastore.save(new Music(1, "Oldstyle", cal1));
        mus_datastore.save(new Music(2, "Jumpstyle", cal2));
        mus_datastore.save(new Music(3, "Rawstyle", cal3));
        mus_datastore.save(new Music(4, "Hardstyle", cal4));
    }

    @Test
    public void testOrderBy(){
//MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore stu_datastore = morphia.createDatastore(new MongoClient(), "student");

        //Order Byクエリ desc
        final Query<Student> orderByDESCQuery = stu_datastore.createQuery(Student.class)
                .order("-id");

        //クエリを発行
        List<Student> res1 = orderByDESCQuery.asList();

        //Order Byクエリ asc
        final Query<Student> orderByASCQuery = stu_datastore.createQuery(Student.class)
                .order("id");

        //クエリを発行
        List<Student> res2 = orderByASCQuery.asList();

        //クエリを発行した結果、レコードが正しい順番で保存されているかをassertThatで確かめる
        assertThat(res1.get(0), is(new Student(4, "Mike")));
        assertThat(res1.get(1), is(new Student(3, "John")));
        assertThat(res1.get(2), is(new Student(2, "Jack")));
        assertThat(res1.get(3), is(new Student(1, "Tom")));

        //クエリを発行した結果、レコードが正しい順番で保存されているかをassertThatで確かめる
        assertThat(res2.get(0), is(new Student(1, "Tom")));
        assertThat(res2.get(1), is(new Student(2, "Jack")));
        assertThat(res2.get(2), is(new Student(3, "John")));
        assertThat(res2.get(3), is(new Student(4, "Mike")));
    }

    @Test
    public void testBetween(){
// MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore stu_datastore = morphia.createDatastore(new MongoClient(), "student");
        final Datastore mus_datastore = morphia.createDatastore(new MongoClient(), "music");

        // Betweenクエリ 数値間でテスト
        final Query<Student> betweenNumQuery = stu_datastore.createQuery(Student.class)
                .filter("id >", 2)
                .filter("id <=", 4);

        // クエリを発行
        List<Student> res1 = betweenNumQuery.asList();

        // 正しいレコードが正しい数で保存されているか確かめる
        assertThat(res1.size(), is(2));
        assertThat(res1.get(0), is(new Student(3, "John")));
        assertThat(res1.get(1), is(new Student(4, "Mike")));

        // Betweenクエリ 日付間でテスト
        final Date from = new GregorianCalendar(2000, 1, 1).getTime();
        final Date to = new GregorianCalendar(2010, 1, 1).getTime();
        final Query<Music> betweenDateQuery = mus_datastore.createQuery(Music.class)
                .field("release").greaterThan(from)
                .field("release").lessThan(to);

        // クエリを発行
        List<Music> res2 = betweenDateQuery.asList();

        // 指定した期間に該当するレコードが取得出来たか確認
        final Date date1 = new GregorianCalendar(2000, 10, 3).getTime();
        final Date date2 = new GregorianCalendar(2005, 6, 4).getTime();
        assertThat(res2.size(), is(2));
        assertThat(res2.get(0), is(new Music(2, "Jumpstyle", date1)));
        assertThat(res2.get(1), is(new Music(3, "Rawstyle", date2)));
    }

    @Test
    public void testIn(){
        // MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore stu_datastore = morphia.createDatastore(new MongoClient(), "student");

        // In句
        final Query<Student> inQuery = stu_datastore.createQuery(Student.class)
                .filter("id in", Arrays.asList(1, 3, 4));

        // クエリを発行
        List<Student> res = inQuery.asList();

        // 正しいレコードが正しい数で保存されているか確かめる
        assertThat(res.size(), is(3));
        assertThat(res.get(0), is(new Student(1, "Tom")));
        assertThat(res.get(1), is(new Student(3, "John")));
        assertThat(res.get(2), is(new Student(4, "Mike")));
    }

    @Test
    public void testOffset(){
        // MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore stu_datastore = morphia.createDatastore(new MongoClient(), "student");

        // Offset(mongoDB上ではskip)句
        final Query<Student> offsetQuery = stu_datastore.createQuery(Student.class)
                .offset(1);

        final Query<Student> mixQuery = stu_datastore.createQuery(Student.class)
                .order("id")
                .offset(1)
                .limit(2);

        // クエリを発行
        List<Student> res1 = offsetQuery.asList();

        // 確認
        assertThat(res1.size(), is(3));
        assertThat(res1.get(0), is(new Student(2, "Jack")));
        assertThat(res1.get(1), is(new Student(3, "John")));
        assertThat(res1.get(2), is(new Student(4, "Mike")));

        // 二つ目のクエリ発行
        List<Student> res2 = mixQuery.asList();

        // 正しいレコードが正しい数で保存されているか確かめる
        assertThat(res2.size(), is(2));
        assertThat(res2.get(0), is(new Student(2, "Jack")));
        assertThat(res2.get(1), is(new Student(3, "John")));
    }

    @Test
    public void testLimit(){
        // MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore stu_datastore = morphia.createDatastore(new MongoClient(), "student");

        // Limitクエリ
        final Query<Student> limitQuery = stu_datastore.createQuery(Student.class)
                .limit(2);

        // クエリを発行
        List<Student> res = limitQuery.asList();

        // 正しいレコードが正しい数で保存されているか確かめる
        assertThat(res.size(), is(2));
        assertThat(res.get(0), is(new Student(1, "Tom")));
        assertThat(res.get(1), is(new Student(2, "Jack")));
    }
}
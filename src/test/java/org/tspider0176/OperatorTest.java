package org.tspider0176;

import com.mongodb.MongoClient;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class OperatorTest{
    // それぞれのoperatorテストを行うための初期データをこの関数内で挿入する
    @Before
    public void initialize(){
        final Morphia morphia = new Morphia();

        // データベースごと初期化する
        MongoClient mongoClient = new MongoClient();
        mongoClient.dropDatabase("student");
        mongoClient.dropDatabase("music");

        // Student用の初期データ挿入
        final Datastore studentDatastore = morphia.createDatastore(new MongoClient(), "student");

        studentDatastore.save(new Student(1, "Tom"));
        studentDatastore.save(new Student(2, "Jack"));
        studentDatastore.save(new Student(3, "John"));
        studentDatastore.save(new Student(4, "Mike"));

        // betweenテストの為の初期データ挿入
        final Datastore musicDatastore = morphia.createDatastore(new MongoClient(), "music");

        final Date cal1 = Date.from(LocalDateTime.of(1995, 4, 15, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date cal2 = Date.from(LocalDateTime.of(2000, 10, 3, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date cal3 = Date.from(LocalDateTime.of(2005, 6, 4, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date cal4 = Date.from(LocalDateTime.of(2010, 5, 29, 0 ,0 ,0).toInstant(ZoneOffset.UTC));

        musicDatastore.save(new Music(1, "Oldstyle", cal1));
        musicDatastore.save(new Music(3, "Rawstyle", cal3));
        musicDatastore.save(new Music(2, "Jumpstyle", cal2));
        musicDatastore.save(new Music(4, "Hardstyle", cal4));
    }

    @Test
    public void testOrderByForNumber(){
        //MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore studentDatastore = morphia.createDatastore(new MongoClient(), "student");

        //Order Byクエリ desc
        final Query<Student> orderByDESCQuery = studentDatastore.createQuery(Student.class)
                .order("-id");

        //クエリを発行
        List<Student> res1 = orderByDESCQuery.asList();

        //Order Byクエリ asc
        final Query<Student> orderByASCQuery = studentDatastore.createQuery(Student.class)
                .order("id");

        //クエリを発行
        List<Student> res2 = orderByASCQuery.asList();

        //クエリを発行した結果、レコードが正しい順番で保存されているかをassertThatで確かめる
        assertThat(res1, is(Arrays.asList(
                        new Student(4, "Mike"),
                        new Student(3, "John"),
                        new Student(2, "Jack"),
                        new Student(1, "Tom")))
        );


        //クエリを発行した結果、レコードが正しい順番で保存されているかをassertThatで確かめる
        assertThat(res2, is(Arrays.asList(
                        new Student(1, "Tom"),
                        new Student(2, "Jack"),
                        new Student(3, "John"),
                        new Student(4, "Mike")))
        );
    }

    @Test
    public void testOrderByForData(){
        //MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore musicDatastore = morphia.createDatastore(new MongoClient(), "music");

        //日付に対するOrder Byクエリ desc
        final Query<Music> orderByDESCQuery = musicDatastore.createQuery(Music.class)
                .order("-release");

        //クエリを発行
        List<Music> res1 = orderByDESCQuery.asList();

        // 日付に対するOrder Byクエリ asc
        final Query<Music> orderByASCQuery = musicDatastore.createQuery(Music.class)
                .order("release");

        //クエリを発行
        List<Music> res2 = orderByASCQuery.asList();

        final Date cal1 = Date.from(LocalDateTime.of(1995, 4, 15, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date cal2 = Date.from(LocalDateTime.of(2000, 10, 3, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date cal3 = Date.from(LocalDateTime.of(2005, 6, 4, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date cal4 = Date.from(LocalDateTime.of(2010, 5, 29, 0 ,0 ,0).toInstant(ZoneOffset.UTC));

        //クエリを発行した結果、レコードが正しい順番で保存されているかをassertThatで確かめる
        assertThat(res1, is(Arrays.asList(
                        new Music(4, "Hardstyle", cal4),
                        new Music(3, "Rawstyle", cal3),
                        new Music(2, "Jumpstyle", cal2),
                        new Music(1, "Oldstyle", cal1)))
        );

        //クエリを発行した結果、レコードが正しい順番で保存されているかをassertThatで確かめる
        assertThat(res2, is(Arrays.asList(
                        new Music(1, "Oldstyle", cal1),
                        new Music(2, "Jumpstyle", cal2),
                        new Music(3, "Rawstyle", cal3),
                        new Music(4, "Hardstyle", cal4)))
        );
    }

    @Test
    public void testOrderByForString(){
        //MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore studentDatastore = morphia.createDatastore(new MongoClient(), "student");

        //Order Byクエリ desc
        final Query<Student> orderByDESCQuery = studentDatastore.createQuery(Student.class)
                .order("-name");

        //クエリを発行
        List<Student> res1 = orderByDESCQuery.asList();

        //Order Byクエリ asc
        final Query<Student> orderByASCQuery = studentDatastore.createQuery(Student.class)
                .order("name");

        //クエリを発行
        List<Student> res2 = orderByASCQuery.asList();

        //クエリを発行した結果、レコードが正しい順番で保存されているかをassertThatで確かめる
        assertThat(res1, is(Arrays.asList(
                        new Student(1, "Tom"),
                        new Student(4, "Mike"),
                        new Student(3, "John"),
                        new Student(2, "Jack")))
        );

        //クエリを発行した結果、レコードが正しい順番で保存されているかをassertThatで確かめる
        assertThat(res2, is(Arrays.asList(
                        new Student(2, "Jack"),
                        new Student(3, "John"),
                        new Student(4, "Mike"),
                        new Student(1, "Tom")))
        );
    }

    @Test
    public void testBetweenByOperator(){
        // MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore studentDatastore = morphia.createDatastore(new MongoClient(), "student");
        final Datastore musicDatastore = morphia.createDatastore(new MongoClient(), "music");

        // Betweenクエリ
        // 数値間でテスト
        final Query<Student> betweenNumQuery = studentDatastore.createQuery(Student.class)
                .filter("id >", 2)
                .filter("id <=", 4);

        // クエリを発行
        List<Student> res1 = betweenNumQuery.asList();

        // 正しいレコードが正しい数で保存されているか確かめる
        assertThat(res1.size(), is(2));
        assertThat(res1, is(Arrays.asList(
                        new Student(3, "John"),
                        new Student(4, "Mike")))
        );

        // Betweenクエリ
        // 日付間でテスト filterの不等号
        final Date from = Date.from(LocalDateTime.of(2000, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date to = Date.from(LocalDateTime.of(2010, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Query<Music> betweenDateQuery = musicDatastore.createQuery(Music.class)
                .filter("release >", from)
                .filter("release <=", to);

        // クエリを発行
        List<Music> res2 = betweenDateQuery.asList();

        // 指定した期間に該当するレコードが取得出来たか確認
        final Date date1 = Date.from(LocalDateTime.of(2000, 10, 3, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date date2 = Date.from(LocalDateTime.of(2005, 6, 4, 0, 0, 0).toInstant(ZoneOffset.UTC));
        assertThat(res2.size(), is(2));
        assertThat(res2, is(Arrays.asList(
                        new Music(3, "Rawstyle", date2),
                        new Music(2, "Jumpstyle", date1)))
        );
    }

    //field().greaterThan()/lessThan() でbetween
    @Test
    public void testBetweenByDSL(){
        // MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore studentDatastore = morphia.createDatastore(new MongoClient(), "student");
        final Datastore musicDatastore = morphia.createDatastore(new MongoClient(), "music");

        // Betweenクエリ idが 2 < id <= 4 を満たすレコードを取得
        final Query<Student> betweenNumQuery = studentDatastore.createQuery(Student.class)
                .field("id").greaterThan(2)
                .field("id").lessThanOrEq(4);

        //クエリを発行
        List<Student> res1 = betweenNumQuery.asList();

        //指定したクエリ通りのレコードが取得できるか確認
        assertThat(res1.size(), is(2));
        assertThat(res1, is(Arrays.asList(
                        new Student(3, "John"),
                        new Student(4, "Mike")))
        );

        // Betweenクエリ releaseが 2000年1月1日以降かつ2010年1月1日未満のレコードを取得
        final Date from = Date.from(LocalDateTime.of(2000, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date to = Date.from(LocalDateTime.of(2010, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Query<Music> betweenDateQuery = musicDatastore.createQuery(Music.class)
                .field("release").greaterThan(from)
                .field("release").lessThanOrEq(to);

        // クエリを発行
        List<Music> res = betweenDateQuery.asList();

        // 指定した期間に該当するレコードが取得出来たか確認
        final Date date1 = Date.from(LocalDateTime.of(2000, 10, 3, 0, 0, 0).toInstant(ZoneOffset.UTC));
        final Date date2 = Date.from(LocalDateTime.of(2005, 6, 4, 0, 0, 0).toInstant(ZoneOffset.UTC));
        assertThat(res.size(), is(2));
        assertThat(res, is(Arrays.asList(
                        new Music(3, "Rawstyle", date2),
                        new Music(2, "Jumpstyle", date1)))
        );
    }

    @Test
    public void testIn(){
        // MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore studentDatastore = morphia.createDatastore(new MongoClient(), "student");

        // In句
        final Query<Student> inQuery = studentDatastore.createQuery(Student.class)
                .filter("id in", Arrays.asList(1, 3, 4));

        // クエリを発行
        List<Student> res = inQuery.asList();

        // 正しいレコードが正しい数で保存されているか確かめる
        assertThat(res.size(), is(3));
        assertThat(res, is(Arrays.asList(
                        new Student(1, "Tom"),
                        new Student(3, "John"),
                        new Student(4, "Mike")))
        );
    }

    @Test
    public void testLimit(){
        // MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore studentDatastore = morphia.createDatastore(new MongoClient(), "student");

        // Limitクエリ
        final Query<Student> limitQuery = studentDatastore.createQuery(Student.class)
                .limit(2);

        // クエリを発行
        List<Student> res = limitQuery.asList();

        // 正しいレコードが正しい数で保存されているか確かめる
        assertThat(res.size(), is(2));
        assertThat(res, is(Arrays.asList(
                        new Student(1, "Tom"),
                        new Student(2, "Jack")))
        );
    }

    @Test
    public void testOffset(){
        // MongoDB側に接続する。
        final Morphia morphia = new Morphia();
        final Datastore studentDatastore = morphia.createDatastore(new MongoClient(), "student");

        // Offset(mongoDB上ではskip)句
        final Query<Student> offsetQuery = studentDatastore.createQuery(Student.class)
                .offset(1);

        final Query<Student> mixQuery = studentDatastore.createQuery(Student.class)
                .order("id")
                .offset(1)
                .limit(2);

        // クエリを発行
        List<Student> res1 = offsetQuery.asList();

        // 確認
        assertThat(res1.size(), is(3));
        assertThat(res1, is(Arrays.asList(
                        new Student(2, "Jack"),
                        new Student(3, "John"),
                        new Student(4, "Mike")))
        );

        // 二つ目のクエリ発行
        List<Student> res2 = mixQuery.asList();

        // 正しいレコードが正しい数で保存されているか確かめる
        assertThat(res2.size(), is(2));
        assertThat(res2, is(Arrays.asList(
                        new Student(2, "Jack"),
                        new Student(3, "John")))
        );
    }
}
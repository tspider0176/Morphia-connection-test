package org.tspider0176;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.Date;
import java.util.Objects;

@Entity("students")
public class Music {
    @Id
    private Integer id;

    private String title;
    private Date release;

    //コンストラクタ
    public Music(){
    }

    public Music(Integer id, String title, Date release){
        this.id = id;
        this.title = title;
        this.release = release;
    }

    //equalsメソッド
    public boolean equals(Object target) {
        if (this == target) return true;
        else if (!(target instanceof Music)) {
            return false;
        } else {
            Music castedTarget = (Music) target;
            return Objects.equals(this.id, castedTarget.id) &&
                    Objects.equals(this.title, castedTarget.title) &&
                    Objects.equals(this.release, castedTarget.release);
        }
    }

    //toStringメソッド
    @Override
    public String toString(){
        return title + "(" + id + ")" + "release at " + release.toString();
    }
}

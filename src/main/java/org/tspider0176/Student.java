package org.tspider0176;

import org.mongodb.morphia.annotations.*;

import java.util.Objects;

@Entity("students")
public class Student {
    @Id
    private Integer id;
    private String name;

    public Student() {
    }

    public Student(Integer id, String name){
        this.id = id;
        this.name = name;
    }

    public boolean equals(Object target) {
        if (this == target) return true;
        else if (!(target instanceof Student)) {
            return false;
        } else {
            Student castedTarget = (Student) target;
            return Objects.equals(this.id, castedTarget.id) &&
                    Objects.equals(this.name, castedTarget.name);
        }
    }

    public String toString(){
        return "Student(" + id + ": " + name + ")";
    }
}

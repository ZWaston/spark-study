package com.zw.spark.study.core.java.advancedprogramming.SecondarySort;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 自定义的二次排序key
 *  1 5
 *  2 4
 *  3 6
 *  1 3
 *  2 1
 *  对上述进行排序，先按照第一列进行排序，若第一列相同的按照第二列进行排序
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>,Serializable{


    private static final long serialVersionUID = 5546531821250190785L;

    //首先在自定义key里面，定义需要进行排序的列
    private int first;
    private int second;

    @Override
    public int compare(SecondarySortKey other) {
        if(this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else {
            return this.second - other.getSecond();
        }
    }

    public boolean $less(SecondarySortKey other) {
        if(this.first < other.getFirst()) {
            return true;
        }else if(this.first == other.getFirst() && this.second < other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey other) {
        if(this.first > other.getFirst()){
            return true;
        }else if(this.first == other.getFirst() && this.second > other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey other) {
        if(this.$less(other)) {
            return true;
        }else if(this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey other) {
        if(this.$greater(other)) {
            return true;
        }else if(this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SecondarySortKey other) {
        if(this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else {
            return this.second - other.getSecond();
        }
    }

    //为要进行排序的列，提供getter和setter方法、构造方法，以及hashcode和equals方法
    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }
}

package com.db;
import com.db.connect;
public class Main {
    public static void main(String[] args) {

        try {
            connect db = new connect();
            db.createTable();
            db.test();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
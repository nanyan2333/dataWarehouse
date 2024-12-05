package com.db;

import com.clickhouse.jdbc.ClickHouseDataSource;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;


public class connect {
    private final String csvPath = System.getProperty("user.dir") + "\\src\\main\\resources\\sale.csv";
    private final Statement stmt;
    private final Connection conn;

    connect() throws SQLException {
        Properties properties = new Properties();
        String url = "jdbc:clickhouse://192.168.11.133/default";
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        this.conn = dataSource.getConnection("default", "123456");
        this.stmt = conn.createStatement();
    }

    public void createTable() {
        int batchSize = 1000;

        try {
            stmt.execute("create database if not exists sale");
            // 根据title数组创建表sale_data，列名为title数组内容
            String createTable = "create table if not exists sale.data(" +
                    "`index` Int32," +
                    "event_time DateTime," +
                    "order_id String," +
                    "product_id String," +
                    "category_id String," +
                    "category_code String," +
                    "brand String," +
                    "price Float64," +
                    "user_id String," +
                    "age Int8," +
                    "sex String," +
                    "local String)" +
                    "ENGINE = MergeTree()" +
                    "ORDER BY `index`;";
            stmt.execute(createTable);
            String insertSQL = "INSERT INTO sale.data (`index`, event_time, order_id, product_id, category_id, category_code, brand, price, user_id, age, sex, local) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement ps = this.conn.prepareStatement(insertSQL);
            DataInputStream in = new DataInputStream(Files.newInputStream(Paths.get(csvPath)));
            CSVReader reader = new CSVReader(new InputStreamReader(in,"gbk"));

            int count = 0;
            // 读取 CSV 文件并跳过标题行
            reader.readNext(); // 跳过标题行

            String[] line;
            while ((line = reader.readNext()) != null) {
                ps.setInt(1, Integer.parseInt(line[0]));
                ps.setString(2, line[1].replace(" UTC", "")); // 直接以 yyyy-MM-dd HH:mm:ss 格式插入
                ps.setString(3, line[2]);
                ps.setString(4, line[3]);
                ps.setString(5, line[4]);
                ps.setString(6, line[5]);
                ps.setString(7, line[6]);
                ps.setDouble(8, Double.parseDouble(line[7]));
                ps.setString(9, line[8]);
                ps.setInt(10, Integer.parseInt(line[9]));
                ps.setString(11, line[10]);
                ps.setString(12, line[11]);
                ps.addBatch(); // 将当前行添加到批次
                count++;

                // 当达到批量大小时，执行批次并清空
                if (count % batchSize == 0) {
                    ps.executeBatch();
                    System.out.println("Inserted " + count + " rows...");
                }
            }

            // 插入剩余的行
            if (count % batchSize != 0) {
                ps.executeBatch();
                System.out.println("Inserted " + count + " rows...");
            }

        } catch (SQLException | IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    public void test() throws SQLException {
        ResultSet rs = stmt.executeQuery("select * from sale.data limit 10;");
        while (rs.next()) {
            System.out.println(rs.getString(11));
        }
    }

}

package com.db;

import com.clickhouse.jdbc.ClickHouseDataSource;

import java.awt.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.data.category.DefaultCategoryDataset;
import javax.swing.*;


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
            stmt.execute("drop table if exists sale.data");
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
            Set<String> date = new HashSet<>();
            Set<String> category = new HashSet<>();
            Set<String> brand = new HashSet<>();
            Set<String> age = new HashSet<>();
            Set<String> location = new HashSet<>();
            String[] line;
            while ((line = reader.readNext()) != null) {
                ps.setInt(1, Integer.parseInt(line[0])); //index
                ps.setString(2, line[1].replace(" UTC", "")); // event_time
                ps.setString(3, line[2]); //order_id
                ps.setString(4, line[3]); //product_id,
                ps.setString(5, line[4]); //category_id,
                ps.setString(6, line[5]); //category_code,
                ps.setString(7, line[6]); //brand,
                ps.setDouble(8, Double.parseDouble(line[7])); //price,
                ps.setString(9, line[8]); //user_id,
                ps.setInt(10, Integer.parseInt(line[9])); //age,
                ps.setString(11, line[10]); //sex,
                ps.setString(12, line[11]); //local
                ps.addBatch(); // 将当前行添加到批次
                count++;
                date.add(line[1].split(" ")[0]);
                category.add(line[5]);
                brand.add(line[6]);
                age.add(line[9]);
                location.add(line[11]);
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

            stmt.execute("create table if not exists sale.vd_date(id Int8,date Date) ENGINE = MergeTree() order by id");

            for (int i = 0; i < date.size(); i++) {
                // 插入数据
                stmt.execute("insert into sale.vd_date values(" + i + ",'" + date.toArray()[i] + "')");
            }
            // 输出提示信息
            System.out.println("Date inserted successfully!");
            stmt.execute("create table if not exists sale.vd_category(id Int8,category String) ENGINE = MergeTree() order by id");
            // 插入数据和id
            for (int i = 0; i < category.size(); i++) {
                // 插入数据
                stmt.execute("insert into sale.vd_category values(" + i + ",'" + category.toArray()[i] + "')");
            }
            System.out.println("category inserted successfully!");
            stmt.execute("create table if not exists sale.vd_brand(id Int8,brand String) ENGINE = MergeTree() order by id");
            // 插入数据和id
            for (int i = 0; i < brand.size(); i++) {
                // 插入数据
                stmt.execute("insert into sale.vd_brand values(" + i + ",'" + brand.toArray()[i] + "')");
            }
            System.out.println("brand inserted successfully!");
            stmt.execute("create table if not exists sale.vd_age(id Int8,age Int8) ENGINE = MergeTree() order by id");
            // 插入数据和id
            for (int i = 0; i < age.size(); i++) {
                // 插入数据
                stmt.execute("insert into sale.vd_age values(" + i + ",'" + age.toArray()[i] + "')");
            }
            System.out.println("age inserted successfully!");
            stmt.execute("create table if not exists sale.vd_location(id Int8,location String) ENGINE = MergeTree() order by id");
            // 插入数据和id
            for (int i = 0; i < location.size(); i++) {
                // 插入数据
                stmt.execute("insert into sale.vd_location values(" + i + ",'" + location.toArray()[i] + "')");
            }
            System.out.println("location inserted successfully!");

        } catch (SQLException | IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    public void test() throws SQLException {
        // 调用com.db.sql的findByLocation.sql文件，执行
        
        ResultSet resultSet = stmt.executeQuery("SELECT src.local as location,SUM(src.price) AS total_price FROM sale.data AS src GROUP BY src.local;");
        // 准备数据集
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        while (resultSet.next()) {
            String category = resultSet.getString("location");
            int value = resultSet.getInt("total_price");
            dataset.addValue(value, "销售额", category);
        }

        // 创建图表
        JFreeChart chart = ChartFactory.createBarChart(
                "地区销售额",       // 图表标题
                "地区",              // x轴标签
                "销售额",                 // y轴标签
                dataset,                 // 数据集
                org.jfree.chart.plot.PlotOrientation.VERTICAL,
                true,                    // 是否显示图例
                true,                    // 是否生成工具提示
                false                    // 是否生成URL链接
        );
        // 设置中文字体
        Font font = new Font("Microsoft YaHei", Font.PLAIN, 14); // 确保系统有该字体
        chart.getTitle().setFont(font); // 标题
        chart.getLegend().setItemFont(font); // 图例
        CategoryPlot plot = chart.getCategoryPlot();
        plot.getDomainAxis().setLabelFont(font); // x轴标签
        plot.getDomainAxis().setTickLabelFont(font); // x轴刻度
        plot.getRangeAxis().setLabelFont(font); // y轴标签
        // 显示图表
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));
        JFrame frame = new JFrame("ClickHouse Data Visualization");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(chartPanel);
        frame.pack();
        frame.setVisible(true);
    }



}

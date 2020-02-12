package edu.fgcu.dataengineering;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import jdk.swing.interop.SwingInterOpUtils;
// just to check if these are found

public class BookStoreDB {

  Statement stmt;
  Connection conn;
  public void initializeDB() {
    final String DB_URL = "jdbc:sqlite:./src/Data/BookStore.db";
    try {

      // STEP 1: Register JDBC driver
      System.out.println("Connecting to Database...");

      //STEP 2: Open a connection

      conn = DriverManager.getConnection(DB_URL);

      stmt = conn.createStatement();
      System.out.println("Successfully connected to Database.");
    } catch (SQLException e) {

      e.printStackTrace();


    }

  }

  public void insertToAuthorDB() throws SQLException, FileNotFoundException {
    System.out.println("Inserting into Author Database...");
    String insertIntoAuthor = "INSERT INTO author (author_name, author_email, author_url) VALUES (?, ?, ?)";
    PreparedStatement ps = conn.prepareStatement(insertIntoAuthor);
    Gson gson = new Gson();
    JsonReader jread = new JsonReader(new FileReader("src/Data/authors.json"));
    AuthorParser[] authors = gson.fromJson(jread, AuthorParser[].class);
    for (var element : authors) {
      ps.setString(1, element.getName());
      ps.setString(2, element.getEmail());
      ps.setString(3, element.getUrl());
      ps.executeUpdate();
    }
    System.out.println("Successfully added data.");
    ps.close();
  }

  public void insertToBookDB() throws SQLException {
    System.out.println("Inserting into Book DB...");
    String insertIntoBook = "INSERT INTO book (isbn, publisher_name, author_name, book_year, book_title, book_price) VALUES (?, ?, ?, ?, ?, ?)";
    PreparedStatement ps = conn.prepareStatement(insertIntoBook);
    Path bookStoreFile = Paths.get("src/Data/bookstore_report2.csv");
    if (Files.exists(bookStoreFile)) {
      try {
        Scanner scanner = new Scanner(bookStoreFile);
        while(scanner.hasNextLine()) {
          String data = scanner.nextLine();
          String[] csvData = data.split(",");
          ps.setString(1, csvData[0]);
          ps.setString(2, csvData[3]);
          ps.setString(3, csvData[2]);
          ps.setString(4, null);
          ps.setString(5, csvData[1]);
          ps.setString(6, null);
          ps.executeUpdate();
          //System.out.println(csvData[0]);
        }
        System.out.println("Successfully inserted.");
        ps.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    } else {
      System.out.println("The file does not exist");
    }
  }



}

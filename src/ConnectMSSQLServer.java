package project;
import java.io.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;


public class ConnectMSSQLServer
{
   public void dbConnect(Connection conn, String SQL)
   {
      try {
         Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
         Statement statement = conn.createStatement();
         ResultSet rs = statement.executeQuery(SQL);
         ResultSetMetaData rsmd = rs.getMetaData();
         int columnsNumber = rsmd.getColumnCount();

         while (rs.next()) {
        	 for (int i = 1; i <= columnsNumber; i++) {
                 if (i > 1) System.out.print(",  ");
                 String columnValue = rs.getString(i);
                 System.out.print(columnValue + " " + rsmd.getColumnName(i));
             }
             System.out.println("");
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public static void readQuery1(Connection con,ConnectMSSQLServer conn, String pathloc) throws IOException, SQLException
   {
	   String aSQLScriptFilePath = "../database/"+pathloc; // Given relative path to the sql scripts. Need to edit if not working.
	   BufferedReader br = null;
		FileReader fr = null;
		
	    fr = new FileReader(aSQLScriptFilePath);
		br = new BufferedReader(fr);

		String sCurrentLine ="";
		String SQL="";

		while ((sCurrentLine = br.readLine()) != null) {
			SQL=SQL+sCurrentLine;
		}
		conn.dbConnect(con, SQL);   
		br.close();
   }
   public static void readQuery2(Connection con,ConnectMSSQLServer conn, String pathloc) throws IOException, SQLException
   {
	   String aSQLScriptFilePath = "C:/Users/Issac/Desktop/Projects/database/"+pathloc;
	   BufferedReader br = null;
		FileReader fr = null;
		
	    fr = new FileReader(aSQLScriptFilePath);
		br = new BufferedReader(fr);

		String sCurrentLine ="";
		String SQL="";

		while ((sCurrentLine = br.readLine()) != null) {
			SQL=SQL+sCurrentLine;
		}
		conn.dbConnect(con, SQL);   
		br.close();
   }
   public static void readQuery3(Connection con,ConnectMSSQLServer conn, String pathloc) throws IOException, SQLException
   {
	   String aSQLScriptFilePath = "C:/Users/Issac/Desktop/Projects/database/"+pathloc;
	   BufferedReader br = null;
		FileReader fr = null;
		
	    fr = new FileReader(aSQLScriptFilePath);
		br = new BufferedReader(fr);

		String sCurrentLine ="";
		String SQL="";

		while ((sCurrentLine = br.readLine()) != null) {
			SQL=SQL+sCurrentLine;
		}
		conn.dbConnect(con, SQL);   
		br.close();
   }
   public static void readQuery4(Connection con,ConnectMSSQLServer conn, String pathloc) throws IOException, SQLException
   {
	   String aSQLScriptFilePath = "C:/Users/Issac/Desktop/Projects/database/"+pathloc;
	   BufferedReader br = null;
		FileReader fr = null;
		
	    fr = new FileReader(aSQLScriptFilePath);
		br = new BufferedReader(fr);

		String sCurrentLine ="";
		String SQL="";

		while ((sCurrentLine = br.readLine()) != null) {
			SQL=SQL+sCurrentLine;
		}
		conn.dbConnect(con, SQL);   
		br.close();
   }
   
   public static void getTables(Connection con,ConnectMSSQLServer conn)
   {
	   
	   try {
           DatabaseMetaData dbmd = con.getMetaData();
           String[] types = {"TABLE"};
           ResultSet rs = dbmd.getTables(null, null, "%", types);
//           int tableNumber = dbmd.getColumnCount();
           int i =1;
           while (rs.next()) {
               System.out.println(i+":"+rs.getString("TABLE_NAME"));
               i++;
           }
       } 
           catch (SQLException e) {
           e.printStackTrace();
       }

	   
   }
   
   public static int getColumnNo(Connection con, ConnectMSSQLServer conn, String table) throws SQLException
   {
	   Statement statement = con.createStatement();
       ResultSet rs = statement.executeQuery("Select * from "+table+";");
       ResultSetMetaData rsmd = rs.getMetaData();
       int columnsNumber = rsmd.getColumnCount();
       return columnsNumber;
	   
   }
   
   public static String findColumnname(Connection con, ConnectMSSQLServer conn, int v, String table) throws SQLException
   {
	   Statement statement = con.createStatement();
	   ResultSet rs = statement.executeQuery("Select * from "+table+";");
	   ResultSetMetaData rsmd = rs.getMetaData(); 
	   return rsmd.getColumnName(v);
   }

   public static String getColumnNameh(Connection con, ConnectMSSQLServer conn, String table) throws SQLException
   {
	   Statement statement = con.createStatement();
	   ResultSet rs = statement.executeQuery("Select * from "+table+";");
       ResultSetMetaData rsmd = rs.getMetaData();
       int columnsNumber = rsmd.getColumnCount();
       String test = "INSERT INTO "+table+" (";
      	 for (int i = 1; i <= columnsNumber; i++) {
               test =test+ rsmd.getColumnName(i);
               System.out.println(i+":"+rsmd.getColumnName(i));
              if(i!=columnsNumber)
               test = test+", ";
       }
      	 test =test+")";
      	// System.out.println(test);
       return test;
   }
   
   public static String getValuesno(int c)
   {
	   String here =" VALUES (";
	   for(int i=1;i<=c;i++)
	   {
		   here = here+"?";
		   if(i!=c)
               here = here+", ";
	   }
	   here = here + ");";
//	   System.out.println(here);
return here;
   }
   
   public static void functionInsert() throws SQLException,IOException
   {
	   ConnectMSSQLServer connServer = new ConnectMSSQLServer();
	      String connectionURL = "jdbc:sqlserver://localhost;databaseName=NORTHWND;integratedSecurity=true";
	      Connection conn = DriverManager.getConnection(connectionURL); 
//	      Statement statement = conn.createStatement();
	      Scanner reader = new Scanner(System.in);
	      String Input;
	      int count =0;
	      System.out.println("\nPlease enter y to insert rows. ");
	      Input = reader.nextLine();
	      while (Input.equalsIgnoreCase("Y"))
	      { 
	    	  count++;
	      System.out.println("\n"+"Which table?");
	      getTables(conn, connServer);
	      System.out.println("\n"+"Enter the table no");
	      int n = reader.nextInt();
	      String table="";
	      switch(n)
	      {
	      case 1 : table = "Categories";break;
	      case 2 : table = "CustomerCustomerDemo";break;
	      case 3 : table = "CustomerDemographics";break;
	      case 4 : table = "Customers";break;
	      case 5 : table = "Employees";break;
	      case 6 : table = "EmployeeTerritories";break;
	      case 7 : table = "Order Details";break;
	      case 8 : table = "Orders";break;
	      case 9 : table = "Products";break;
	      case 10 : table = "Region";break;
	      case 11 : table = "Shippers";break;
	      case 12 : table = "Suppliers";break;
	      case 13 : table = "sysdiagrams";break;
	      case 14 : table = "Territories";break;
	      case 15 : table = "trace_xe_action_map";break;
	      case 16 : table = "trace_xe_event_map";break;  
	      }
	      System.out.println("The chosen table is "+table+"\n");
	      System.out.println("The columns are");

	boolean setIdentOn = true;
	      String SQLinsert = (setIdentOn ? "set identity_insert " + table + " on " : "");  
	      int c= getColumnNo(conn, connServer, table);
	      String columnames= getColumnNameh(conn,connServer,table);
	      String values = getValuesno(c);
	      String news = SQLinsert+columnames+values;

	      System.out.println("\nEnter the "+c+" column values of "+table);
	      PreparedStatement preparedStatement = conn.prepareStatement(news);
	     for(int i=1;i<=c;i++)
	     {
	      preparedStatement.setString(i, reader.next());
	     }
	     try
	     {
	     preparedStatement.executeUpdate();      
	      
	      System.out.println("Do you want to continue (y or n)");
	      Input = reader.next();
	   }
	   catch (SQLException e) {
	       //e.printStackTrace();
	       System.out.println("Exception in the sql statment, do you want to continue (y or n)");
	       Input = reader.next();
	       if(count>0)count--;
	   }
	      }
	   System.out.println("\n"+count+" no of rows inserted");
//	   reader.close();
   }
   
   public static void functionUpdate() throws SQLException,IOException
   {
	   ConnectMSSQLServer connServer = new ConnectMSSQLServer();
	      String connectionURL = "jdbc:sqlserver://localhost;databaseName=NORTHWND;integratedSecurity=true";
	      Connection conn = DriverManager.getConnection(connectionURL); 
//	      Statement statement = conn.createStatement();
	      Scanner reader = new Scanner(System.in);
	      String Input1;
	      int count =0;
	   System.out.println("Please enter y to update rows. ");
	      Input1 = reader.next();
	      System.out.println(Input1);

	      while (Input1.equalsIgnoreCase("Y"))
	      { 
	    	  count++;
	      System.out.println("\n"+"Which table?");
	      getTables(conn, connServer);
	      System.out.println("\n"+"Enter the table no");
	      int n = reader.nextInt();
	      String table="";
	      switch(n)
	      {
	      case 1 : table = "Categories";break;
	      case 2 : table = "CustomerCustomerDemo";break;
	      case 3 : table = "CustomerDemographics";break;
	      case 4 : table = "Customers";break;
	      case 5 : table = "Employees";break;
	      case 6 : table = "EmployeeTerritories";break;
	      case 7 : table = "Order Details";break;
	      case 8 : table = "Orders";break;
	      case 9 : table = "Products";break;
	      case 10 : table = "Region";break;
	      case 11 : table = "Shippers";break;
	      case 12 : table = "Suppliers";break;
	      case 13 : table = "sysdiagrams";break;
	      case 14 : table = "Territories";break;
	      case 15 : table = "trace_xe_action_map";break;
	      case 16 : table = "trace_xe_event_map";break;  
	      }
	      System.out.println("The chosen table is "+table+"\n");
	      System.out.println("Enter the colomn to be updated");
	      getColumnNameh(conn,connServer,table);     
	      int tobeUpd = reader.nextInt();
	      if(tobeUpd ==1)
	      {
	    	 System.out.println("\nCannot update the primary key. Try again!");
	      }
	      else
	      {
	      String columnname= findColumnname(conn,connServer, tobeUpd, table);
	      String columnname1= findColumnname(conn,connServer, 1, table);
	      
	      System.out.println(columnname);
	      System.out.println("Enter the value to be updated");
	      String tobeUpdv = reader.next();
	      
	      System.out.println("Enter the id to which this value should be updated");
	      String tobeUpdid = reader.next();
	      
	      String updateSQL = "Update "+table+" Set "+columnname+"='"+tobeUpdv+"' where "+columnname1+"="+tobeUpdid+";";
	      PreparedStatement preparedStatement = conn.prepareStatement(updateSQL);
	     try
	     {
	     preparedStatement.executeUpdate();            
	      System.out.println("Do you want to continue (y or n)");
	      Input1 = reader.next();
	   }
	   catch (SQLException e) {
	       //e.printStackTrace();
	       System.out.println("Exception in the sql statment, do you want to continue (y or n)");
	       Input1 = reader.next();
	       if(count>0)count--;
	   }
	      }
	      }
	   System.out.println("\n"+count+" no of rows updated");  
	   reader.close();
   }

  public static void functionSQL() throws SQLException, IOException
   {
	   ConnectMSSQLServer connServer = new ConnectMSSQLServer();
	      String connectionURL = "jdbc:sqlserver://localhost;databaseName=NORTHWND;integratedSecurity=true";
	      Connection conn = DriverManager.getConnection(connectionURL); 

	      System.out.println("SQL Query 1"+"\n");
	      readQuery1(conn, connServer,"Query1.sql");
	      System.out.println("\n"+"SQL Query 2"+"\n");
	      readQuery2(conn, connServer,"Query1.sql");
	      System.out.println("\n"+"SQL Query 3"+"\n");
	      readQuery3(conn, connServer,"Query3.sql");
	      System.out.println("\n"+"SQL Query 4"+"\n");
	      readQuery4(conn, connServer,"Query4.sql");    
	   
   }

   public static void main(String[] args) throws IOException, SQLException
   {   
      functionSQL();
      functionInsert();
      functionUpdate();
   }
}
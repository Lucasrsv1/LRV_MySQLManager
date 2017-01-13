/// CREATED BY LRV;

/// This software is for test the MySQLManager library;
/// To use it, please create the next users in your DBMS or change the connection configuration below:
/// 'Manager'@'%' with password "12345678" and all DML privilegies;
/// 'Manager'@'localhost' with password "12345678" and all DML privilegies;
///
/// And then, run the next SQL script:
/// CREATE DATABASE IF NOT EXISTS test;
/// USE test;
/// CREATE TABLE IF NOT EXISTS teste (
///		id int PRIMARY KEY AUTO_INCREMENT,
///		string VARCHAR(100) NULL
/// );

using LRV_Utilities.DBMS;
using System;

namespace MySQLManager_Tests {
	class Program {
		const string NULL = MySQLManager.NULL;

		static void ShowNonQueryResult (MySQLManager dbms) {
			Console.WriteLine("Query Type: " + dbms.CurrentDML + " [OK]");
			Console.WriteLine("Query: " + dbms.Query + " [OK]");

			if (dbms.AffectedRows == -1) {
				if (dbms.Error.Number != -1)
					Console.WriteLine("Error " + dbms.Error.Number + ": " + dbms.Error.Message + " [!]");

				if (dbms.Error.MySqlExceptionNumber != -1)
					Console.WriteLine("MySql error " + dbms.Error.MySqlExceptionNumber + ": " + dbms.Error.InnerException.Message + " [!]\n");
				else
					Console.WriteLine("Exception message: " + dbms.Error.InnerException.Message + " [!]\n");
			} else {
				Console.WriteLine("Affected Rows: " + dbms.AffectedRows + " [OK]\n");
			}
		}

		static void ShowQueryResult (MySQLManager dbms) {
			Console.WriteLine("Query Type: " + dbms.CurrentDML + " [OK]");
			Console.WriteLine("Query: " + dbms.Query + " [OK]");

			if (dbms.SelectResult != null) {
				string columnsJoint = "";
				foreach (string column in dbms.SelectResult[0])
					columnsJoint += "| " + column + " ";

				columnsJoint += "|";

				Console.WriteLine("Table Name: " + dbms.TableName + " [OK]");
				Console.WriteLine("Num rows selected: " + dbms.NumRows + " [OK]");
				Console.WriteLine("Num columns selected: " + dbms.SelectResult[0].Length + " [OK]");

				Console.WriteLine("Select result:\n");
				Console.WriteLine(columnsJoint);

				for (int row = 1; row <= dbms.NumRows; row++) {
					Console.Write("| ");
					for (int column = 0; column < dbms.SelectResult[0].Length; column++)
						Console.Write(dbms.SelectResult[row][column] + " | ");

					Console.Write("\n");
				}
				Console.WriteLine("\nSelect completed [OK]\n");
			} else {
				if (dbms.Error.Number != -1)
					Console.WriteLine("Error " + dbms.Error.Number + ": " + dbms.Error.Message + " [!]");

				if (dbms.Error.MySqlExceptionNumber != -1)
					Console.WriteLine("MySql error " + dbms.Error.MySqlExceptionNumber + ": " + dbms.Error.InnerException.Message + " [!]\n");
				else if (dbms.Error.InnerException != null)
					Console.WriteLine("Exception message: " + dbms.Error.InnerException.Message + " [!]\n");
			}
		}

		static void ShowConnectionStatus (MySQLManager dbms) {
			Console.WriteLine("Connection String: " + dbms.ConnectionString + " [OK]");
			Console.WriteLine("Connection Status: " + ((dbms.Connected) ? "CONNECTED [OK]\n" : "DISCONNECTED [!]\n"));
			if (dbms.Error != null) {
				if (dbms.Error.Number != -1)
					Console.WriteLine("Error " + dbms.Error.Number + ": " + dbms.Error.Message + " [!]");

				if (dbms.Error.MySqlExceptionNumber != -1)
					Console.WriteLine("MySql error " + dbms.Error.MySqlExceptionNumber + ": " + dbms.Error.InnerException.Message + " [!]\n");
				else
					Console.WriteLine("Exception message: " + dbms.Error.InnerException.Message + " [!]\n");

				Console.ReadKey();
				return;
			}
		}

		static void Main (string[] args) {
			MySQLManager dbms;
			Console.WriteLine("Starting [...]");

			dbms = new MySQLManager("test", "Manager", "12345678"); //Connection
			ShowConnectionStatus(dbms);

			string tableName = "teste";
			string[] columns = new string[] { "string", "id" };
			string[][] values = new string[][] { new string[] { "Hello" }, new string[] { "World" } };

			dbms.InsertInto(tableName, columns, values);
			ShowNonQueryResult(dbms);

			dbms.Select("SELECT COUNT(*) FROM `teste`");
			ShowQueryResult(dbms);

			dbms.DeleteFrom("DELETE FROM teste;");
			ShowNonQueryResult(dbms);

			dbms.InsertInto("INSERT INTO teste(id) VALUES (75), (76), (77), (78), (79), (80);");
			ShowNonQueryResult(dbms);

			dbms.Update("UPDATE teste SET id = 73 WHERE id = 77;");
			ShowNonQueryResult(dbms);

			dbms.Update(tableName, new string[] { "string" }, new string[] { "TEST" }, "id = 73");
			ShowNonQueryResult(dbms);

			dbms.Select(tableName, new string[] { "string" }, "id < 77", "ORDER BY id DESC");
			ShowQueryResult(dbms);

			dbms.InsertInto(tableName, new string[] { "string" }, dbms.SelectResult, 1);
			ShowNonQueryResult(dbms);

			dbms.Select("SELECT `id` AS 'Num', `string` FROM `teste` ORDER BY id DESC LIMIT 0, 1");
			ShowQueryResult(dbms);

			dbms.DeleteFrom(tableName, "id = " + dbms.SelectResult[0][1]); //[First Column][First Register]
			ShowNonQueryResult(dbms);

			dbms.Select("SELECT COUNT(*) FROM teste");
			ShowQueryResult(dbms);

			Console.WriteLine("Finished [OK]");
			Console.ReadKey();
		}
	}
}
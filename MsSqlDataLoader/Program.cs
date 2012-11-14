using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace MsSqlDataLoader
{
	class Program
	{
		static int Main(string[] args)
		{
			if (args.Length != 1)
			{
				ShowUsage();
				return 1;
			}

			var connString = args[0];

			try
			{
				var conn = new SqlConnection(connString);
				conn.Open();

				foreach (var ti in GetTables(conn))
				{
					Console.Write("Load table {0} - ", ti.ToString());
					int consoleCol = Console.CursorLeft;
					Console.Write("0");

					var file = ti.Schema + "." + ti.Name + ".sql";
					bool isAutoIncrement = CheckIdentity(conn, ti);

					string query = string.Format("select * from {0}", ti);

					var cmd = new SqlCommand(query, conn);

					var sb = new StringBuilder();

					int total = 0;

					using (var reader = cmd.ExecuteReader())
					{
						var cols = reader.FieldCount;
						var colNames = Enumerable.Range(0, cols).Select(i => "[" + reader.GetName(i) + "]");
						var insertHeader = string.Format("INSERT {0} ({1}) VALUES", ti, string.Join(", ", colNames));

						var textRows = new List<string>();

						while (reader.Read())
						{
							textRows.Add(RowToValues(Enumerable.Range(0, cols).Select(reader.GetValue)));

							if (textRows.Count >= 100)
							{
								total += textRows.Count;
								Write(sb, insertHeader, textRows);
								textRows.Clear();

								Console.CursorLeft = consoleCol;
								Console.Write(total);
							}
						}


						total += textRows.Count;
						Write(sb, insertHeader, textRows);
						Console.CursorLeft = consoleCol;
						Console.Write(total);
					}

					if (sb.Length != 0)
					{
						if (isAutoIncrement)
						{
							sb.Insert(0, string.Format("SET IDENTITY_INSERT {0} ON\r\n\r\n", ti));
							sb.Append(string.Format("SET IDENTITY_INSERT {0} OFF\r\n", ti));
						}
						File.WriteAllText(file, sb.ToString());

						Console.WriteLine(" saved.");
					}
					else
					{
						Console.WriteLine();
					}
				}

				return 0;
			}
			catch (Exception ex)
			{
				Console.WriteLine();
				Console.WriteLine("Error: {0}", ex);
				return 1;
			}
		}

		private static void ShowUsage()
		{
			Console.WriteLine("Usage: MsSqlDataLoader [connectionString]");
			Console.WriteLine("Example:");
			Console.WriteLine("  MsSqlDataLoader \"Server=localhost;Database=Northwind;User Id=sa;Password=sa;\"");
		}

		public static List<TableInfo> GetTables(SqlConnection conn)
		{
			DataTable schema = conn.GetSchema("Tables");
			var result = new List<TableInfo>();
			foreach (DataRow row in schema.Rows)
			{
				if(string.Equals("BASE TABLE", row[3].ToString(), StringComparison.InvariantCultureIgnoreCase))
					result.Add(new TableInfo() {Name = row[2].ToString(), Schema = row[1].ToString()});
			}
			return result;
		}

		private static bool CheckIdentity(SqlConnection conn, TableInfo ti)
		{
			string cmdText = string.Format("SELECT TOP(1) * FROM {0}", ti);

			var dataTable = new DataTable();
			new SqlDataAdapter(cmdText, conn).FillSchema(dataTable, SchemaType.Source);

			return dataTable.Columns.Cast<DataColumn>().Any(col => col.AutoIncrement);
		}

		private static void Write(StringBuilder sb, string insertHeader, List<string> textRows)
		{
			if(textRows.Count == 0)
				return;

			sb.Append(insertHeader);
			sb.AppendLine();
			sb.Append(textRows[0]);
			foreach(var line in textRows.Skip(1))
			{
				sb.Append(",\r\n");
				sb.Append(line);
			}
			sb.AppendLine();
			sb.Append("GO");
			sb.AppendLine();
		}

		private static string RowToValues(IEnumerable<object> values)
		{
			return "(" + string.Join(", ", values.Select(CellToSQL)) + ")";
		}

		private static string CellToSQL(object val)
		{
			Type t = val.GetType();
			if (t == typeof(DBNull))
				return "NULL";
			if (t == typeof(bool))
				return (bool)val?"1":"0";

			if (t == typeof(string))
			{
				var text = (string) val;
				text = text.Replace("'", "''");

				if (IsASCII(text))
					return "'" + text + "'";
				else
					return "N'" + text + "'";
			}

			if (t == typeof(DateTime))
				return "'" + ((DateTime)val).ToString("yyyy.MM.dd HH:mm:ss.fff") + "'";

			if (t == typeof(Int16))
				return val.ToString();

			if (t == typeof(Int32))
				return val.ToString();

			if (t == typeof(Int64))
				return val.ToString();

			if (t == typeof(byte))
				return val.ToString();

			if (t == typeof(decimal))
				return ((decimal)val).ToString(CultureInfo.InvariantCulture);

			if (t == typeof(double))
				return ((double)val).ToString(CultureInfo.InvariantCulture);

			if (t == typeof(float))
				return ((float)val).ToString(CultureInfo.InvariantCulture);

			if (t == typeof(Guid))
				return "'" + ((Guid)val).ToString() + "'";

			if (t == typeof(Byte[]))
				return ByteArrayToString((Byte[])val);

			throw new NotImplementedException("unexpected type: " + t.FullName);
		}

		public static string ByteArrayToString(byte[] ba)
		{
			var hex = new StringBuilder(ba.Length * 2);
			hex.Append("0x");
			foreach (byte b in ba)
				hex.AppendFormat("{0:x2}", b);
			return hex.ToString();
		}

		private static bool IsASCII(string text)
		{
			return text.All(ch => (int) ch <= 255);
		}
	}
}

using Business.Interfaces;
using Business.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Business.Services
{
    public class Connect : Iconnect
    {
        readonly IConfiguration _iconfiguration;

        public Connect(IConfiguration iconfiguration)
        {
            _iconfiguration = iconfiguration;
        }

        public Task<bool> ConnectDb(Connection connection)
        {
            throw new NotImplementedException();
        }

        public async Task<List<SourceTables>> GetDatabseDetails(Connection connection)
        {
            if (connection.DBServerType == "PostGreConnection")
                return GetPostGreSource(connection);
            else if (connection.DBServerType == "MysqlConnection")
                return GetMySqlSource(connection);
            else if (connection.DBServerType == "SqlConnection")
                return GetSqlSource(connection);
            else
            {
                return GetPostGreSource(connection);
            }
        }

        #region Postgre SQL 
        private List<SourceTables> GetPostGreSource(Connection connection)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(connection.DBServerType).Value;
            str = String.Format(str, connection.UserId, connection.Password);

            List<SourceTables> lsttables = new List<SourceTables>();

            NpgsqlConnection conn = new NpgsqlConnection(str);
            conn.Open();
            NpgsqlCommand command = new NpgsqlCommand("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'AND table_type = 'BASE TABLE'", conn);

            NpgsqlDataReader dr = command.ExecuteReader();
            // Output rows
            while (dr.Read())
            {
                SourceTables sourceTables = new SourceTables();
                sourceTables.Table = dr["table_name"].ToString();
                sourceTables.Columns = GetPostGreSourceColumns(connection,dr["table_name"].ToString());
                lsttables.Add(sourceTables);
            }
            conn.Close();

            return lsttables;
        }

        private List<string> GetPostGreSourceColumns(Connection connection, string tableName)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(connection.DBServerType).Value;
            str = String.Format(str, connection.UserId, connection.Password);

            List<string> lstColumns = new List<string>();
            // Connect to a PostgreSQL database
            NpgsqlConnection conn = new NpgsqlConnection(str);
            conn.Open();

            NpgsqlCommand command = new NpgsqlCommand("SELECT column_name FROM information_schema.columns WHERE table_name   = '" + tableName + "'", conn);

            NpgsqlDataReader dr = command.ExecuteReader();

            // Output rows
            while (dr.Read())
            {
                lstColumns.Add(dr["column_name"].ToString());
            }
            conn.Close();

            return lstColumns;
        }
        #endregion

        #region MySql

        // get Table name and Columns data for MY SQL.
        private List<SourceTables> GetMySqlSource(Connection connection)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(connection.DBServerType).Value;
            str = String.Format(str, connection.UserId, connection.Password);

            List<SourceTables> lsttables = new List<SourceTables>();

            MySqlConnection conn = new MySqlConnection(str);
            conn.Open();
            MySqlCommand command = new MySqlCommand("show tables from testdb", conn);

            MySqlDataReader dr = command.ExecuteReader();
            // Output rows
            while (dr.Read())
            {
                SourceTables sourceTables = new SourceTables();
                sourceTables.Table = dr["Tables_in_testdb"].ToString();
                sourceTables.Columns = GetMySqlSourceColumns(connection, dr["Tables_in_testdb"].ToString());
                lsttables.Add(sourceTables);
            }
            conn.Close();

            return lsttables;
        }
        private List<string> GetMySqlSourceColumns(Connection connection, string tableName)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(connection.DBServerType).Value;
            str = String.Format(str, connection.UserId, connection.Password);

            List<string> lstColumns = new List<string>();
            // Connect to a ,MySQL database
            MySqlConnection conn = new MySqlConnection(str);
            conn.Open();

            MySqlCommand command = new MySqlCommand("SELECT column_name  FROM information_schema.columns WHERE table_name ='" + tableName + "'", conn);

            MySqlDataReader dr = command.ExecuteReader();

            // Output rows
            while (dr.Read())
            {
                lstColumns.Add(dr["column_name"].ToString());
            }
            conn.Close();

            return lstColumns;
        }

        #endregion

        #region SQL

        // Get SQL tables and Coulumns list
        private List<SourceTables> GetSqlSource(Connection connection)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(connection.DBServerType).Value;
            str = String.Format(str, connection.UserId, connection.Password);

            List<SourceTables> lsttables = new List<SourceTables>();
            // Connect to a SQL database
            SqlConnection conn = new SqlConnection(str);
            conn.Open();
            SqlCommand command = new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", conn);

            SqlDataReader dr = command.ExecuteReader();
            // Output rows
            while (dr.Read())
            {
                SourceTables sourceTables = new SourceTables();
                sourceTables.Table = dr["table_name"].ToString();
                sourceTables.Columns = GetMySqlSourceColumns(connection, dr["table_name"].ToString());
                lsttables.Add(sourceTables);
            }
            conn.Close();

            return lsttables;
        }
        private List<string> GetSqlSourceColumns(Connection connection, string tableName)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(connection.DBServerType).Value;
            str = String.Format(str, connection.UserId, connection.Password);

            List<string> lstColumns = new List<string>();
            
            SqlConnection conn = new SqlConnection(str);
            conn.Open();

            SqlCommand command = new SqlCommand("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema   = '" + tableName + "'", conn);

            SqlDataReader dr = command.ExecuteReader();

            // Output rows
            while (dr.Read())
            {
                lstColumns.Add(dr["column_name"].ToString());
            }
            conn.Close();

            return lstColumns;
        }

        #endregion
    }
}

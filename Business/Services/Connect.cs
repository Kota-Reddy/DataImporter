using Business.Interfaces;
using Business.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
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
            return GetSourceTables(connection);
        }
        private List<SourceTables> GetSourceTables(Connection connection)
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
                sourceTables.Columns = GetSourceColumns(connection,dr["table_name"].ToString());
                lsttables.Add(sourceTables);
            }
            conn.Close();

            return lsttables;
        }

        private List<string> GetSourceColumns(Connection connection, string tableName)
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
    }
}

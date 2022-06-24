using Business.Interfaces;
using Business.Model;
using Business.DTOs;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Npgsql;
using System.Data.SqlClient;

namespace Business.Services
{
    public class TableDetailsService : ITableDetailsService
    {
        readonly IConfiguration _iconfiguration;

        public TableDetailsService(IConfiguration iconfiguration)
        {
            _iconfiguration = iconfiguration;
        }

        public Task<bool> ConnectDb(DataBaseSchema dataBaseSchema)
        {
            throw new NotImplementedException();
        }

        public async Task<List<TablesSchemaDto>> GetDatabseDetails(DataBaseSchema dataBaseSchema)
        {
            List<TablesSchemaDto> listsourcetables = new List<TablesSchemaDto>();

            if (dataBaseSchema.DBServerType == "PostGreConnection")
                listsourcetables = GetPostGreSource(dataBaseSchema);
            else if (dataBaseSchema.DBServerType == "MysqlConnection")
                listsourcetables = GetMySqlSource(dataBaseSchema);
            else if (dataBaseSchema.DBServerType == "MsSqlConnection")
                listsourcetables = GetMsSqlSource(dataBaseSchema);

            return listsourcetables;
        }

        #region Postgre SQL 
        private List<TablesSchemaDto> GetPostGreSource(DataBaseSchema dataBaseSchema)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(dataBaseSchema.DBServerType).Value;
            str = String.Format(str, dataBaseSchema.UserId, dataBaseSchema.Password);

            List<TablesSchemaDto> lsttables = new List<TablesSchemaDto>();

            NpgsqlConnection conn = new NpgsqlConnection(str);
            conn.Open();
            NpgsqlCommand command = new NpgsqlCommand("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'AND table_type = 'BASE TABLE'", conn);

            NpgsqlDataReader dr = command.ExecuteReader();
            // Output rows
            while (dr.Read())
            {
                TablesSchemaDto tablesSchemaDto = new TablesSchemaDto();
                tablesSchemaDto.TableName = dr["table_name"].ToString();
                tablesSchemaDto.ColumnsDetails = GetPostGreSourceColumns(dataBaseSchema, dr["table_name"].ToString());
                lsttables.Add(tablesSchemaDto);
            }
            conn.Close();

            return lsttables;
        }

        private List<ColumnsDetails> GetPostGreSourceColumns(DataBaseSchema dataBaseSchema, string tableName)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(dataBaseSchema.DBServerType).Value;
            str = String.Format(str, dataBaseSchema.UserId, dataBaseSchema.Password);

            List<ColumnsDetails> lstColumnsDetails = new List<ColumnsDetails>();
            // Connect to a PostgreSQL database
            NpgsqlConnection conn = new NpgsqlConnection(str);
            conn.Open();

            NpgsqlCommand command = new NpgsqlCommand("SELECT column_name,data_type,character_maximum_length FROM information_schema.columns WHERE table_name   = '" + tableName + "'", conn);

            NpgsqlDataReader dr = command.ExecuteReader();

            // Output rows
            while (dr.Read())
            {
                ColumnsDetails columnsDetails = new ColumnsDetails();
                columnsDetails.ColumnName = dr["column_name"].ToString();
                columnsDetails.IsPrimaryKey = true;
                columnsDetails.DataType = dr["data_type"].ToString();
                columnsDetails.Length = dr["character_maximum_length"].ToString();
                lstColumnsDetails.Add(columnsDetails);
            }
            conn.Close();

            return lstColumnsDetails;
        }
        #endregion

        #region MySql.

        // get Table name and Columns data for MY SQL.
        private List<TablesSchemaDto> GetMySqlSource(DataBaseSchema dataBaseSchema)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(dataBaseSchema.DBServerType).Value;
            str = String.Format(str, dataBaseSchema.UserId, dataBaseSchema.Password);

            List<TablesSchemaDto> lsttables = new List<TablesSchemaDto>();

            MySqlConnection conn = new MySqlConnection(str);
            conn.Open();
            MySqlCommand command = new MySqlCommand("show tables from testdb", conn);

            MySqlDataReader dr = command.ExecuteReader();
            // Output rows
            while (dr.Read())
            {
                TablesSchemaDto tablesSchemaDto = new TablesSchemaDto();
                tablesSchemaDto.TableName = dr["table_name"].ToString();
                tablesSchemaDto.ColumnsDetails = GetMySqlSourceColumns(dataBaseSchema, dr["table_name"].ToString());
                lsttables.Add(tablesSchemaDto);
            }
            conn.Close();

            return lsttables;
        }
        
        /// <summary>
        /// Get SQL tables and Coulumns list.
        /// </summary>
        /// <remarks>
        /// Created by : kota.reddy,
        /// Created on : 23/06/2022,
        /// Purpose : Get MySQL tables and Coulumns list,
        /// </remarks>
        /// <param name=""></param>
        /// <returns></returns>
        private List<ColumnsDetails> GetMySqlSourceColumns(DataBaseSchema dataBaseSchema, string tableName)
        {
            string str = _iconfiguration.GetSection("Data").GetSection(dataBaseSchema.DBServerType).Value;
            str = String.Format(str, dataBaseSchema.UserId, dataBaseSchema.Password);

            List<ColumnsDetails> lstColumnsDetails = new List<ColumnsDetails>();
            // Connect to a ,MySQL database
            MySqlConnection conn = new MySqlConnection(str);
            conn.Open();

            MySqlCommand command = new MySqlCommand("SELECT column_name,data_type,character_maximum_length  FROM information_schema.columns WHERE table_name ='" + tableName + "'", conn);

            MySqlDataReader dr = command.ExecuteReader();

            // Output rows
            while (dr.Read())
            {
                ColumnsDetails columnsDetails = new ColumnsDetails();
                columnsDetails.ColumnName = dr["column_name"].ToString();
                columnsDetails.IsPrimaryKey = false;
                columnsDetails.DataType = dr["data_type"].ToString();
                columnsDetails.Length = dr["character_maximum_length"].ToString();
                lstColumnsDetails.Add(columnsDetails);
            }
            conn.Close();

            return lstColumnsDetails;
        }

        #endregion

        #region MsSQL

        /// <summary>
        /// Get SQL tables and Coulumns list.
        /// </summary>
        /// <remarks>
        /// Created by  :   Sunil Thakur,
        /// Created on  :   20/06/2022,
        /// Purpose     :   Get SQL tables and Coulumns list,
        /// </remarks>
        /// <param name=""></param>
        /// <returns></returns>
        private List<TablesSchemaDto> GetMsSqlSource(DataBaseSchema dataBaseSchema)
        {
            List<TablesSchemaDto> listtablesSchemaDto = new List<TablesSchemaDto>();

            try
            {
                string str = _iconfiguration.GetSection("Data").GetSection(dataBaseSchema.DBServerType).Value;
                str = String.Format(str, dataBaseSchema.UserId, dataBaseSchema.Password);

                // Connect to a SQL database
                SqlConnection conn = new SqlConnection(str);
                conn.Open();

                SqlCommand command = new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", conn);
                SqlDataReader dr = command.ExecuteReader();

                // Output rows
                while (dr.Read())
                {
                    TablesSchemaDto tablesSchemaDto = new TablesSchemaDto();

                    tablesSchemaDto.TableName = dr["table_name"].ToString();
                    tablesSchemaDto.ColumnsDetails = GetMsSqlSourceColumns(dataBaseSchema, dr["table_name"].ToString());

                    listtablesSchemaDto.Add(tablesSchemaDto);
                }
                conn.Close();
            }
            catch (Exception ex)
            {
                string exe = ex.Message;
            }

            return listtablesSchemaDto;
        }   //GetSqlSource

        /// <summary>
        /// Get SQL tables and Coulumns list and it's details from database.
        /// </summary>
        /// <remarks>
        /// Created by  :   Sunil Thakur,
        /// Created on  :   20/06/2022,
        /// Purpose     :   Get SQL tables and Coulumns list and it's details from database.
        /// </remarks>
        /// <param name=""></param>
        /// <returns></returns>
        private List<ColumnsDetails> GetMsSqlSourceColumns(DataBaseSchema dataBaseSchema, string tableName)
        {
            List<ColumnsDetails> listColumnsDetails = new List<ColumnsDetails>();

            try
            {
                string str = _iconfiguration.GetSection("Data").GetSection(dataBaseSchema.DBServerType).Value;
                str = String.Format(str, dataBaseSchema.UserId, dataBaseSchema.Password);

                SqlConnection conn = new SqlConnection(str);
                conn.Open();

                //SqlCommand command = new SqlCommand("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME   = '" + tableName + "'", conn);
                SqlCommand command = new SqlCommand("SELECT  C.COLUMN_NAME, C.DATA_TYPE, C.CHARACTER_MAXIMUM_LENGTH, U.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.COLUMNS C FULL OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE U ON C.COLUMN_NAME = U.COLUMN_NAME WHERE C.TABLE_NAME = '" + tableName + "'", conn);

                SqlDataReader dr = command.ExecuteReader();

                // Output rows
                while (dr.Read())
                {
                    ColumnsDetails columnsDetails = new ColumnsDetails();

                    columnsDetails.ColumnName = dr["COLUMN_NAME"].ToString();
                    columnsDetails.IsPrimaryKey = dr["CONSTRAINT_NAME"].ToString().Length > 0 ? true : false;
                    columnsDetails.DataType = dr["DATA_TYPE"].ToString();
                    columnsDetails.Length = dr["CHARACTER_MAXIMUM_LENGTH"].ToString();

                    listColumnsDetails.Add(columnsDetails);
                }
                conn.Close();
            }
            catch (Exception ex)
            {
                string exe = ex.Message;
            }
            return listColumnsDetails;

        }   //GetSqlSourceColumns

        #endregion
    }
}

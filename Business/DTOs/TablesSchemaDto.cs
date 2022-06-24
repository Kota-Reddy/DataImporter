namespace Business.DTOs
{
    public class TablesSchemaDto
    {
        public string TableName { get; set; }
        public List<ColumnsDetails> ColumnsDetails { get; set; }
    }

    public class ColumnsDetails
    {
        public string ColumnName { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string DataType { get; set; }
        public string Length { get; set; }
    }
}

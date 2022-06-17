using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Business.Model
{
    public class Connection
    {
        public string DBServerType { get; set; }
        public string UserId { get; set; }
        public string Password { get; set; }
        public List<SourceTables> sourceTables { get; set; }
    }

    public class SourceTables
    {
        public string Table { get; set; }
        public List<string> Columns { get; set; }
    }
}

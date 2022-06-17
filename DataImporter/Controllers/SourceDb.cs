using Business.Model;
using Business.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DataImporter.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SourceDb : ControllerBase
    {
        readonly IConfiguration _iconfiguration;

        public SourceDb(IConfiguration iconfiguration)
        {
            _iconfiguration = iconfiguration;
        }

        [HttpPost, Route("Data/SourcedbConnect/")]
        public async Task<IActionResult> SourceDbConnect(Connection connection)
        {
            connection.sourceTables = new List<SourceTables>();
            // To Do - 1. Connect Post Gre SqL - Database - get Tables data - get columns data
            Connect connect = new Connect(_iconfiguration);
            connection.sourceTables = await connect.GetDatabseDetails(connection);
            return Ok(connection);
        }
    }
}

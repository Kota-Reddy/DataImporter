using Business.DTOs;
using Business.Model;
using Business.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DataImporter.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DBDetailsController : ControllerBase
    {
        readonly IConfiguration _iconfiguration;

        public DBDetailsController(IConfiguration iconfiguration)
        {
            _iconfiguration = iconfiguration;
        }

        [HttpPost, Route("TableDetailsService/GetDataDaseDetails")]
        public async Task<ActionResult> GetDataDaseDetails(DataBaseSchema dataBaseSchema)
        {
            Business.Services.TableDetailsService tableDetailsService = new Business.Services.TableDetailsService(_iconfiguration);

            List<TablesSchemaDto> tablesSchemaDto = new List<TablesSchemaDto>();

            tablesSchemaDto = await tableDetailsService.GetDatabseDetails(dataBaseSchema);

            return Ok(tablesSchemaDto);
        }
    }
}

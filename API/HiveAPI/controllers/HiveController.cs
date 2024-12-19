using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;

[Route("api/[controller]")]
[ApiController]
public class HiveController : ControllerBase
{
    private string ConnectionString = "Driver={Hive ODBC Driver};Host=localhost;Port=10000;Schema=default;UID=root;PWD=pwd1234;";

    [HttpGet("data")]
    public IActionResult GetData([FromQuery] string query = "SELECT * FROM your_table LIMIT 10")
    {
        try
        {
            using (OdbcConnection connection = new OdbcConnection(ConnectionString))
            {
                connection.Open();
                using (OdbcCommand command = new OdbcCommand(query, connection))
                {
                    using (OdbcDataReader reader = command.ExecuteReader())
                    {
                        var results = new List<Dictionary<string, object>>();

                        while (reader.Read())
                        {
                            var row = new Dictionary<string, object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                row[reader.GetName(i)] = reader.GetValue(i);
                            }
                            results.Add(row);
                        }

                        return Ok(results);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { error = ex.Message });
        }
    }
}

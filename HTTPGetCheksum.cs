using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;

using System.IO.Compression;

using System.Text;

using System.Security.Cryptography;

namespace Company.Function
{
    public static class HTTPGetBase64ToHEX
    {
        [FunctionName("HTTPGetBase64ToHEX")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)]
                HttpRequest req,
            ILogger log
        )
        {
            string fname = await new StreamReader(req.Body).ReadToEndAsync();
            var bytes = Convert.FromBase64String(fname);
            var hex = BitConverter.ToString(bytes);

            return new OkObjectResult(hex.Replace("-", "").ToLower());
        }
    }
}

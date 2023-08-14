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

using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

using System.IO.Compression;

using System.Text;

namespace Company.Function
{
    public class batchCDP
    {
        [JsonProperty("ref")]
        public Guid reff { get; set; }
        public string schema { get; set; }
        public string mode { get; set; }

        public Value value { get; set; }
    }

    public class Value
    {
        public string guestType { get; set; }
        public string firstName { get; set; }
        public string lastName { get; set; }
        public string email { get; set; }

        public List<string> street { get; set; }

        public string city { get; set; }

        public int postCode { get; set; }

        public string state { get; set; }

        public List<Identifiers> identifiers { get; set; }
        public List<Extensions> extensions { get; set; }
    }

    public class Identifiers
    {
        public string provider { get; set; }
        public string id { get; set; }
    }

    public class Extensions
    {
        public string name { get; set; }
        public string key { get; set; }
        public double totalAmountSpent { get; set; }
        public int countPageViewEvent { get; set; }
        public int countVisitedCategoryPageEvent { get; set; }
        public int countVisitedProductDetailsPageEvent { get; set; }
        public string groupName { get; set; }
        
    }

    public static class HttpTrigger
    {
        [FunctionName("HttpTrigger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)]
                HttpRequest req,
            ILogger log
        )
        {

            try{
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
log.LogError(requestBody);
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            string fname = data?.name;
            // Setup the connection to the storage account

            BlobServiceClient blobServiceClient = new BlobServiceClient(
                Environment.GetEnvironmentVariable("conn_blob")
            );

            BlobContainerClient container = blobServiceClient.GetBlobContainerClient("fssyn");

            string sJSON = "";
            var str = Environment.GetEnvironmentVariable("conn");

            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                var text =
                    @"
        SELECT TOP (100) 
                            NEWID() as ref
                            ,[schema]
                            ,[mode]
                            ,[FirstName]
                            ,[LastName]
                            ,[email]
                            ,[street]
                            ,[TotalAmountSpent]
                            ,[CountPageViewEvent]
                            ,[CountVisitedCategoryPageEvent]
                            ,[CountVisitedProductDetailsPageEvent]
                            ,[city]
                            ,[state]
                            ,[postCode]
                            ,[id]
                            ,[provider]
                            ,ISNULL(groupName, 'NA') as [groupName]
                            FROM [sc-data].[dbo].[CustomerModel]
                     ";

                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    using (SqlDataReader reader = await cmd.ExecuteReaderAsync())
                    {
                        while (reader.Read())
                        {
                            var jsonObject = new batchCDP();

                            jsonObject.reff = (Guid)reader["ref"];
                            jsonObject.schema = (string)reader["schema"];
                            jsonObject.mode = (string)reader["mode"];
                            jsonObject.value = new Value()
                            {
                                guestType = "customer",
                                firstName = (string)reader["FirstName"],
                                lastName = (string)reader["LastName"],
                                email = (string)reader["email"],
                                street = new List<String> { (string)reader["street"] },
                                city = (string)reader["city"],
                                state = (string)reader["state"],
                                postCode = (int)reader["postCode"],
                                identifiers = new List<Identifiers>
                                {
                                    new Identifiers()
                                    {
                                        provider = "email",// (string)reader["provider"],
                                        id = (string)reader["email"], //(string)reader["id"]
                                    }
                                },
                                extensions = new List<Extensions>
                                {
                                    new Extensions()
                                    {
                                        name = "ext",
                                        key = "default",
                                        totalAmountSpent = (double)reader["TotalAmountSpent"],
                                        countPageViewEvent = (int)reader["CountPageViewEvent"],
                                        countVisitedProductDetailsPageEvent = (int)
                                            reader["CountVisitedProductDetailsPageEvent"],
                                        countVisitedCategoryPageEvent = (int)
                                            reader["CountVisitedCategoryPageEvent"],
                                            groupName = (string)reader["groupName"]
                                    }
                                }
                            };

                            sJSON += JsonConvert.SerializeObject(jsonObject);
                            sJSON += "\n";
                        }

                        BlobClient blob = container.GetBlobClient(fname);
                        // Get the blob file as text


                        System.Text.ASCIIEncoding encoding = new System.Text.ASCIIEncoding();
                        byte[] bytes = encoding.GetBytes(sJSON);

                        using (MemoryStream ms = new MemoryStream())
                        {
                            using (
                                GZipStream gzip = new GZipStream(ms, CompressionMode.Compress, true)
                            )
                            {
                                gzip.Write(bytes, 0, bytes.Length);
                            }
                            ms.Position = 0;

                            await blob.UploadAsync(
                                ms,
                                new BlobHttpHeaders() { ContentType = "application/x-gzip" }
                            );
                        }
                    }
                }
            }

            return new OkObjectResult("Ok");

            }catch(Exception ex){
    log.LogError(ex, "Something went wrong");
        return new StatusCodeResult(500);

            }
        }
    }
}

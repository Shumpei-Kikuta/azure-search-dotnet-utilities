// This is a prototype tool that allows for extraction of data from a search index
// Since this tool is still under development, it should not be used for production usage

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;

namespace AzureSearchBackupRestoreIndex;

class Program
{
    private static string SourceSearchServiceName;
    private static string SourceAdminKey;
    private static string SourceIndexName;
    private static string TargetSearchServiceName;
    private static string TargetAdminKey;
    private static string TargetIndexName;
    private static string BackupDirectory;

    private static SearchIndexClient SourceIndexClient;
    private static SearchClient SourceSearchClient;
    private static SearchIndexClient TargetIndexClient;
    private static SearchClient TargetSearchClient;

    private static int MaxBatchSize = 500;          // JSON files will contain this many documents / file and can be up to 1000
    private static int ParallelizedJobs = 10;       // Output content in parallel jobs

    static void Main()
    {
        //Get source and target search service info and index names from appsettings.json file
        //Set up source and target search service clients
        ConfigurationSetup();

        //Backup the source index
        Console.WriteLine("\nSTART INDEX BACKUP");
        BackupIndexAndDocuments();

        //Recreate and import content to target index
        Console.WriteLine("\nSTART INDEX RESTORE");
        DeleteIndex();
        CreateTargetIndex();
        ImportFromJSON();
        Console.WriteLine("\n  Waiting 10 seconds for target to index content...");
        Console.WriteLine("  NOTE: For really large indexes it may take longer to index all content.\n");
        Thread.Sleep(10000);

        // Validate all content is in target index
        int sourceCount = GetCurrentDocCount(SourceSearchClient);
        int targetCount = GetCurrentDocCount(TargetSearchClient);
        Console.WriteLine("\nSAFEGUARD CHECK: Source and target index counts should match");
        Console.WriteLine(" Source index contains {0} docs", sourceCount);
        Console.WriteLine(" Target index contains {0} docs\n", targetCount);

        Console.WriteLine("Press any key to continue...");
        Console.ReadLine();
    }

    static void ConfigurationSetup()
    {

        IConfigurationBuilder builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");
        IConfigurationRoot configuration = builder.Build();

        SourceSearchServiceName = configuration["SourceSearchServiceName"];
        SourceAdminKey = configuration["SourceAdminKey"];
        SourceIndexName = configuration["SourceIndexName"];
        TargetSearchServiceName = configuration["TargetSearchServiceName"];
        TargetAdminKey = configuration["TargetAdminKey"];
        TargetIndexName = configuration["TargetIndexName"];
        BackupDirectory = configuration["BackupDirectory"];

        Console.WriteLine("CONFIGURATION:");
        Console.WriteLine("\n  Source service and index {0}, {1}", SourceSearchServiceName, SourceIndexName);
        Console.WriteLine("\n  Target service and index: {0}, {1}", TargetSearchServiceName, TargetIndexName);
        Console.WriteLine("\n  Backup directory: " + BackupDirectory);
        Console.WriteLine("\nDoes this look correct? Press any key to continue, Ctrl+C to cancel.");
        Console.ReadLine();

        SourceIndexClient = new SearchIndexClient(new Uri("https://" + SourceSearchServiceName + ".search.windows.net"), new AzureKeyCredential(SourceAdminKey));
        SourceSearchClient = SourceIndexClient.GetSearchClient(SourceIndexName);


        TargetIndexClient = new SearchIndexClient(new Uri($"https://" + TargetSearchServiceName + ".search.windows.net"), new AzureKeyCredential(TargetAdminKey));
        TargetSearchClient = TargetIndexClient.GetSearchClient(TargetIndexName);
    }

    static void BackupIndexAndDocuments()
    {
        // Backup the index schema to the specified backup directory
        Console.WriteLine("\n Backing up source index schema to {0}\n", Path.Combine(BackupDirectory, SourceIndexName + ".schema"));

        File.WriteAllText(Path.Combine(BackupDirectory, SourceIndexName + ".schema"), GetIndexSchema());

        // Extract the content to JSON files
        int SourceDocCount = GetCurrentDocCount(SourceSearchClient);
        
        // For indexes with more than 100,000 documents, use facets to divide and extract data
        if (SourceDocCount > 100000)
        {
            Console.WriteLine("\n Index contains more than 100,000 documents ({0}). Using facet-based extraction.", SourceDocCount);
            WriteIndexDocumentsUsingFacets();
        }
        else
        {
            WriteIndexDocuments(SourceDocCount);     // Output content from index to json files
        }
    }

    // Extract documents using facets to bypass the 100K limitation
    static void WriteIndexDocumentsUsingFacets()
    {

        var schema = SourceIndexClient.GetIndex(SourceIndexName);
        
        // Find facetable fields that can be used to segment the documents
        // Good facet fields typically have:
        // 1. A reasonable number of distinct values (not too few, not too many)
        // 2. Values that distribute documents somewhat evenly
        // 3. String type or collections of strings are preferred
        var facetableFields = schema.Value.Fields
            .Where(f => f.IsFacetable == true && (f.Type == "Edm.String" || f.Type.StartsWith("Collection")))
            .Select(f => f.Name)
            .ToList();

        if (!facetableFields.Any())
        {
            Console.WriteLine(" Error: No facetable fields found in the index. Please select a field manually.");
            return;
        }

        // Select the optimal facet field based on cardinality analysis
        string selectedFacetField = SelectOptimalFacetField(facetableFields);
        Console.WriteLine(" Using field '{0}' as the facet field for document extraction.", selectedFacetField);

        // Get all facet values for the selected field
        SearchOptions facetOptions = new SearchOptions
        {
            SearchMode = SearchMode.All,
            Facets = { selectedFacetField },
            Size = 0  // We only need facet information, not results
        };

        SearchResults<SearchDocument> facetResults = SourceSearchClient.Search<SearchDocument>("*", facetOptions);
        var facetValues = facetResults.Facets[selectedFacetField].Select(f => f.Value?.ToString()).Where(v => !string.IsNullOrEmpty(v)).ToList();

        Console.WriteLine(" Field '{0}' has {1} distinct values.", selectedFacetField, facetValues.Count);

        // Handle null values separately as they require different filter syntax
        facetValues.Add(null);

        // Export documents for each facet value
        int fileCounter = 0;
        foreach (var facetValue in facetValues)
        {
            fileCounter++;
            string filterExpression;
            
            if (facetValue == null)
            {
                filterExpression = $"{selectedFacetField} eq null";
                Console.WriteLine(" Backing up documents with facet value: null");
            }
            else
            {
                filterExpression = $"{selectedFacetField} eq '{facetValue}'";
                Console.WriteLine(" Backing up documents with facet value: '{0}'", facetValue);
            }

            string fileName = Path.Combine(BackupDirectory, $"{SourceIndexName}{fileCounter}.json");
            ExportToJSONWithFilter(filterExpression, fileName);
        }
    }

    // Helper method to select the optimal facet field based on cardinality and distribution
    static string SelectOptimalFacetField(List<string> facetableFields)
    {
        // Ideal facet field should have:
        // 1. Enough distinct values to break data into manageable chunks (<100K docs per value)
        // 2. Not too many values to avoid excessive processing
        // 3. Even distribution of documents across values

        Dictionary<string, (int valueCount, double evenness)> fieldScores = new Dictionary<string, (int valueCount, double evenness)>();
        
        foreach (var field in facetableFields)
        {
            SearchOptions facetOptions = new SearchOptions
            {
                SearchMode = SearchMode.All,
                Facets = { $"{field},count:1000" }, // Try to get up to 1000 facet values for analysis
                Size = 0
            };

            try
            {
                SearchResults<SearchDocument> facetResults = SourceSearchClient.Search<SearchDocument>("*", facetOptions);
                var facetValues = facetResults.Facets[field];
                
                // Calculate the evenness of distribution (closer to 1.0 is more even)
                double avgCount = facetValues.Sum(f => f.Count ?? 0) / (double)facetValues.Count;
                double variance = facetValues.Sum(f => Math.Pow((f.Count ?? 0) - avgCount, 2)) / facetValues.Count;
                double evenness = 1.0 / (1.0 + Math.Sqrt(variance) / avgCount);

                fieldScores[field] = (facetValues.Count, evenness);
                
                Console.WriteLine(" Field '{0}' has {1} values with evenness score {2:F2}", 
                    field, facetValues.Count, evenness);
            }
            catch (Exception ex)
            {
                Console.WriteLine(" Error analyzing field '{0}': {1}", field, ex.Message);
            }
        }

        // Simple scoring: prefer fields with 10-100 distinct values and high evenness
        string bestField = facetableFields.First();
        double bestScore = 0;
        
        foreach (var kvp in fieldScores)
        {
            int valueCount = kvp.Value.valueCount;
            double evenness = kvp.Value.evenness;
            
            // Penalty for too few or too many values
            double cardinalityScore;
            if (valueCount < 5) cardinalityScore = 0.3;
            else if (valueCount <= 100) cardinalityScore = 1.0;
            else if (valueCount <= 500) cardinalityScore = 0.7;
            else cardinalityScore = 0.4;
            
            double score = cardinalityScore * evenness;
            
            Console.WriteLine(" Field '{0}' score: {1:F2}", kvp.Key, score);
            
            if (score > bestScore)
            {
                bestScore = score;
                bestField = kvp.Key;
            }
        }
        
        Console.WriteLine(" Selected field '{0}' as optimal (score: {1:F2})", bestField, bestScore);
        return bestField;
    }

    // Export documents to JSON using a filter expression
    static void ExportToJSONWithFilter(string filterExpression, string fileName)
    {
        string json = string.Empty;
        List<SearchDocument> allDocuments = new List<SearchDocument>();

        try
        {
            SearchOptions options = new SearchOptions
            {
                SearchMode = SearchMode.All,
                Size = MaxBatchSize,
                Filter = filterExpression
            };

            // Iterate through search results with the specified filter
            int docCount = 0;
            SearchResults<SearchDocument> response;
            
            do
            {
                response = SourceSearchClient.Search<SearchDocument>("*", options);
                var documents = response.GetResults().Select(r => r.Document).ToList();
                allDocuments.AddRange(documents);
                docCount += documents.Count;
                options.Skip = (options.Skip ?? 0) + options.Size;
                
                Console.WriteLine("  Progress: Retrieved {0} documents", docCount);
            } 
            while (response.GetResults().Count > 0);

            // Save all documents in JSON format
            foreach (var doc in allDocuments)
            {
                json += JsonSerializer.Serialize(doc) + ",";
                json = json.Replace("\"Latitude\":", "\"type\": \"Point\", \"coordinates\": [");
                json = json.Replace("\"Longitude\":", "");
                json = json.Replace(",\"IsEmpty\":false,\"Z\":null,\"M\":null,\"CoordinateSystem\":{\"EpsgId\":4326,\"Id\":\"4326\",\"Name\":\"WGS84\"}", "]");
                json += "\n";
            }

            if (allDocuments.Count > 0)
            {
                // Output the formatted content to a file
                json = json.Substring(0, json.Length - 3); // remove trailing comma
                File.WriteAllText(fileName, "{\"value\": [");
                File.AppendAllText(fileName, json);
                File.AppendAllText(fileName, "]}");
                Console.WriteLine("  Total documents: {0}", allDocuments.Count);
            }
            else
            {
                // Create an empty JSON file
                File.WriteAllText(fileName, "{\"value\": []}");
                Console.WriteLine("  No documents found for this facet value");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("  Error: {0}", ex.Message);
        }
    }

    static string GetIDFieldName()
    {
        // Find the id field of this index
        string IDFieldName = string.Empty;
        try
        {
            var schema = SourceIndexClient.GetIndex(SourceIndexName);
            foreach (var field in schema.Value.Fields)
            {
                if (field.IsKey == true)
                {
                    IDFieldName = Convert.ToString(field.Name);
                    break;
                }
            }

        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: {0}", ex.Message);
        }

        return IDFieldName;
    }

    static string GetIndexSchema()
    {
        // Extract the schema for this index
        // We use REST here because we can take the response as-is

        Uri ServiceUri = new Uri("https://" + SourceSearchServiceName + ".search.windows.net");
        HttpClient HttpClient = new HttpClient();
        HttpClient.DefaultRequestHeaders.Add("api-key", SourceAdminKey);

        string Schema = string.Empty;
        try
        {
            Uri uri = new Uri(ServiceUri, "/indexes/" + SourceIndexName);
            HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Get, uri);
            AzureSearchHelper.EnsureSuccessfulSearchResponse(response);
            Schema = response.Content.ReadAsStringAsync().Result;
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: {0}", ex.Message);
        }

        return Schema;
    }

    private static bool DeleteIndex()
    {
        Console.WriteLine("\n  Delete target index {0} in {1} search service, if it exists", TargetIndexName, TargetSearchServiceName);
        // Delete the index if it exists
        try
        {
            TargetIndexClient.DeleteIndex(TargetIndexName);
        }
        catch (Exception ex)
        {
            Console.WriteLine("  Error deleting index: {0}\n", ex.Message);
            Console.WriteLine("  Did you remember to set your SearchServiceName and SearchServiceApiKey?\n");
            return false;
        }

        return true;
    }

    static void CreateTargetIndex()
    {
        Console.WriteLine("\n  Create target index {0} in {1} search service", TargetIndexName, TargetSearchServiceName);
        // Use the schema file to create a copy of this index
        // I like using REST here since I can just take the response as-is

        string json = File.ReadAllText(Path.Combine(BackupDirectory, SourceIndexName + ".schema"));

        // Do some cleaning of this file to change index name, etc
        json = "{" + json.Substring(json.IndexOf("\"name\""));
        int indexOfIndexName = json.IndexOf("\"", json.IndexOf("name\"") + 5) + 1;
        int indexOfEndOfIndexName = json.IndexOf("\"", indexOfIndexName);
        json = json.Substring(0, indexOfIndexName) + TargetIndexName + json.Substring(indexOfEndOfIndexName);

        Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
        HttpClient HttpClient = new HttpClient();
        HttpClient.DefaultRequestHeaders.Add("api-key", TargetAdminKey);

        try
        {
            Uri uri = new Uri(ServiceUri, "/indexes");
            HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, uri, json);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex)
        {
            Console.WriteLine("  Error: {0}", ex.Message);
        }
    }

    static int GetCurrentDocCount(SearchClient searchClient)
    {
        // Get the current doc count of the specified index
        try
        {
            SearchOptions options = new SearchOptions
            {
                SearchMode = SearchMode.All,
                IncludeTotalCount = true
            };

            SearchResults<Dictionary<string, object>> response = searchClient.Search<Dictionary<string, object>>("*", options);
            return Convert.ToInt32(response.TotalCount);
        }
        catch (Exception ex)
        {
            Console.WriteLine("  Error: {0}", ex.Message);
        }

        return -1;
    }

    static void ImportFromJSON()
    {
        Console.WriteLine("\n  Upload index documents from saved JSON files");
        // Take JSON file and import this as-is to target index
        Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
        HttpClient HttpClient = new HttpClient();
        HttpClient.DefaultRequestHeaders.Add("api-key", TargetAdminKey);

        try
        {
            foreach (string fileName in Directory.GetFiles(BackupDirectory, SourceIndexName + "*.json"))
            {
                Console.WriteLine("  -Uploading documents from file {0}", fileName);
                string json = File.ReadAllText(fileName);
                Uri uri = new Uri(ServiceUri, "/indexes/" + TargetIndexName + "/docs/index");
                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, uri, json);
                response.EnsureSuccessStatusCode();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("  Error: {0}", ex.Message);
        }
    }
}

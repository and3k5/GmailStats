using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Gmail.v1;
using Google.Apis.Gmail.v1.Data;
using Google.Apis.Services;
using Google.Apis.Util;
using Google.Apis.Util.Store;
using LiteDB;
using RestSharp;
using FileMode = System.IO.FileMode;

namespace GmailStats
{
    class Program
    {
        class Options
        {
            [Option('c', "client-id", Required = true, HelpText = "client_id.json file")]
            public string ClientIdFilePath { get; set; }

            [Option('q', "gmail-query", Required = true, HelpText = "gmail filter format query")]
            public string Query { get; set; }

            [Option('e', "erase-cache", Required = false, HelpText = "erase cache and fetch all new information")]
            public bool EraseCache { get; set; }
        }

        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed(Run);
            Console.ReadLine();
        }

        private static void Run(Options obj)
        {
            Console.WriteLine("Start");
            string[] scopes = {GmailService.Scope.GmailReadonly};

            Console.WriteLine("Init gmail service");
            var init = new BaseClientService.Initializer
            {
                HttpClientInitializer = CreateAuthorizeAsync(obj.ClientIdFilePath, scopes).Result
            };


            var clientService = new GmailService(init);


            Console.WriteLine("Start message fetcher and list fetcher");
            var cache = new GmailCache();
            if (obj.EraseCache)
                cache.Erase();
            var fetcher = new MessageDetailFetcher(clientService, cache);
            fetcher.Run();

            var listTask = Task.Run(() =>
            {
                var query = obj.Query;
                var requestMaxResults = 1000;
                string pageToken = null;

                var request = ListRequest(clientService, pageToken, requestMaxResults, query);
                executeRequest:
                var response = request.Execute();

                foreach (var responseMessage in response.Messages)
                    fetcher.AddTodo(responseMessage);

                if (response.NextPageToken != null)
                {
                    var nextPageToken = response.NextPageToken;
                    request = ListRequest(clientService, nextPageToken, requestMaxResults, query);
                    goto executeRequest;
                }
            });


            listTask.Wait();
            Console.WriteLine("List task done");
            fetcher.Stop();

            var messages = fetcher.GetResult();
            Console.WriteLine("Fetcher done");

            var counts = new Dictionary<string, int>();

            Console.WriteLine("Result");

            foreach (var message in messages)
            {
                var from = message.From;
                var lastClose = from.LastIndexOf('>');
                var lastOpen = from.LastIndexOf('<');

                var email = from.Substring(lastOpen, lastClose - lastOpen).ToLower();

                if (!counts.ContainsKey(email))
                {
                    counts[email] = 0;
                }

                counts[email]++;
            }

            foreach (var pair in counts.OrderByDescending(x => x.Value).Take(30))
            {
                Console.WriteLine(pair.Key + "       =       " + pair.Value);
            }
        }

        private static UsersResource.MessagesResource.ListRequest ListRequest(GmailService clientService, string pageToken, int requestMaxResults, string query)
        {
            var request = new UsersResource.MessagesResource.ListRequest(clientService, "me");

            if (pageToken != null)
                request.PageToken = pageToken;

            request.MaxResults = requestMaxResults;
            request.Q = query;
            return request;
        }

        private class MessageDetailFetcher
        {
            private volatile bool Exit = false;

            private object TodoLock = new object();
            private List<Message> Todo = new List<Message>();
            private object DoneLock = new object();
            private List<MessageFromItem> Done = new List<MessageFromItem>();
            private IClientService clientService;
            private readonly GmailCache _gmailCache;

            public MessageDetailFetcher(IClientService clientService, GmailCache gmailCache)
            {
                this.clientService = clientService;
                _gmailCache = gmailCache;
            }

            public int TodoCount
            {
                get
                {
                    lock (TodoLock)
                        return Todo.Count;
                }
            }

            private void FetchTask()
            {
                var tasks = new List<Task>();
                while (Exit == false || TodoCount != 0)
                {
                    Message element = null;
                    lock (TodoLock)
                    {
                        element = Todo.Count > 0 ? Todo[0] : null;
                        if (element != null)
                            Todo.Remove(element);
                    }

                    if (element != null)
                    {
                        var cachedElement = _gmailCache.Get(element.Id);
                        if (cachedElement != null)
                        {
                            lock (DoneLock)
                                Done.Add(cachedElement);
                        }
                        else
                        {
                            var request = new UsersResource.MessagesResource.GetRequest(clientService, "me", element.Id);

                            var fetchTask = Task.Run(() =>
                            {
                                var responseMessage = request.Execute();
                                var mess = new MessageFromItem()
                                {
                                    Id = responseMessage.Id,
                                    From = responseMessage.Payload.Headers.Single(x => x.Name.Equals("From", StringComparison.OrdinalIgnoreCase)).Value,
                                };
                                lock (DoneLock)
                                {
                                    Done.Add(mess);
                                }

                                _gmailCache.AddItem(mess);
                            });
                            tasks.Add(fetchTask);
                        }
                    }
                    else
                    {
                        System.Threading.Thread.Sleep(100);
                    }
                }

                Task.WhenAll(tasks).Wait();
            }

            public void Run()
            {
                this.Task = Task.Run(() => FetchTask());
            }

            public void AddTodo(Message message)
            {
                lock (TodoLock)
                    Todo.Add(message);
            }

            public void Stop()
            {
                Exit = true;
            }

            public MessageFromItem[] GetResult()
            {
                if (this.Task == null)
                    throw new Exception("Task is not started");
                this.Task.Wait();
                return this.Done.ToArray();
            }

            public Task Task { get; set; }
        }

        private static Task<UserCredential> CreateAuthorizeAsync(string clientIdFilePath, IEnumerable<string> scopes)
        {
            var fileStream = new FileStream(clientIdFilePath, FileMode.Open, FileAccess.Read);
            return GoogleWebAuthorizationBroker.AuthorizeAsync(
                GoogleClientSecrets.Load(fileStream).Secrets,
                scopes,
                "user",
                CancellationToken.None,
                new LiteDbDataStore("AuthDataStore.db"));
        }
    }
}
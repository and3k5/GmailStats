using System;
using System.Threading.Tasks;
using Google.Apis.Json;
using Google.Apis.Util.Store;
using LiteDB;

namespace GmailStats
{
    public class MessageFromItem
    {
        public string Id { get; set; }
        public string From { get; set; }
    }
    public class GmailCache
    {
        private const string CollectionName = "MessageFromItem";
        private readonly LiteDatabase Database = new LiteDatabase("GmailCache.db");

        public void AddItem(MessageFromItem msg)
        {
            var coll = Database.GetCollection<MessageFromItem>(CollectionName);
            coll.Upsert(msg);
        }

        public MessageFromItem Get(string id)
        {
            var coll = Database.GetCollection<MessageFromItem>(CollectionName);
            return coll.FindById(id);
        }

    }
    public class LiteDbDataStore : IDataStore
    {
        private const string DataStoreCollectionName = "DataStore";

        private class LiteDbDataStoreItem
        {
            public string Id { get; set; }
            public string Data { get; set; }
        }

        private static readonly Task CompletedTask = Task.FromResult(0);

        private readonly string dbPath;

        public LiteDbDataStore(string dbPath)
        {
            this.dbPath = dbPath;
        }

        public Task StoreAsync<T>(string key, T value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key MUST have a value");

            using (var db = new LiteDB.LiteDatabase(dbPath))
            {
                var collection = db.GetCollection<LiteDbDataStoreItem>(DataStoreCollectionName);

                var serialized = NewtonsoftJsonSerializer.Instance.Serialize(value);

                collection.Upsert(new LiteDbDataStoreItem()
                {
                    Id = GenerateStoredKey(key, typeof(T)),
                    Data = serialized,
                });
            }

            return CompletedTask;
        }

        public Task DeleteAsync<T>(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key MUST have a value");

            using (var db = new LiteDB.LiteDatabase(dbPath))
            {
                var collection = db.GetCollection<LiteDbDataStoreItem>(DataStoreCollectionName);

                var idKey = GenerateStoredKey(key, typeof(T));

                collection.Delete(item => item.Id == idKey);
            }

            return CompletedTask;
        }

        public Task<T> GetAsync<T>(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key MUST have a value");


            var tcs = new TaskCompletionSource<T>();

            try
            {
                using (var db = new LiteDB.LiteDatabase(dbPath))
                {
                    var collection = db.GetCollection<LiteDbDataStoreItem>(DataStoreCollectionName);

                    var idKey = GenerateStoredKey(key, typeof(T));

                    var item = collection.FindById(idKey);

                    tcs.SetResult(item == null ? default(T) : NewtonsoftJsonSerializer.Instance.Deserialize<T>(item.Data));
                }
            }
            catch (Exception ex)
            {
                tcs.SetException(ex);
            }

            return tcs.Task;
        }

        public Task ClearAsync()
        {
            using (var db = new LiteDB.LiteDatabase(dbPath))
                if (db.CollectionExists(DataStoreCollectionName))
                    db.DropCollection(DataStoreCollectionName);

            return CompletedTask;
        }

        public static string GenerateStoredKey(string key, Type t)
        {
            return string.Format("{0}-{1}", t.FullName, key);
        }
    }
}
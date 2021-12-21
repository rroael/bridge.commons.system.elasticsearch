using System;
using System.Collections.Generic;
using System.Linq;
using Bridge.Commons.System.Elasticsearch.Contracts;
using Elasticsearch.Net;
using Nest;

namespace Bridge.Commons.System.Elasticsearch
{
    /// <summary>
    ///     Client de elastic search
    /// </summary>
    public class ElasticsearchClient : IElasticsearchClient
    {
        private readonly string _defaultIndex;
        private readonly IList<Uri> _endpoints;
        private ElasticClient _client;
        private ElasticLowLevelClient _lowLevelClient;

        /// <summary>
        ///     Contrutor
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="defaultIndex"></param>
        public ElasticsearchClient(string endpoint, string defaultIndex = null)
        {
            _endpoints = new List<Uri> { new Uri(endpoint) };
            _defaultIndex = defaultIndex;
        }

        /// <summary>
        ///     Construtor
        /// </summary>
        /// <param name="endpoints"></param>
        /// <param name="defaultIndex"></param>
        public ElasticsearchClient(IEnumerable<string> endpoints, string defaultIndex = null)
        {
            _endpoints = endpoints.Select(x => new Uri(x)).ToList();
            _defaultIndex = defaultIndex;
        }

        /// <summary>
        ///     Inicializa
        /// </summary>
        /// <param name="indexes"></param>
        /// <param name="debugProcess"></param>
        /// <param name="connection"></param>
        /// <param name="defaultIndex"></param>
        public void Init(IDictionary<Type, string> indexes = null, Action<IApiCallDetails> debugProcess = null,
            IConnection connection = null, string defaultIndex = null)
        {
            CreateConnection(debugProcess, connection, defaultIndex);
            RegisterIndexes(indexes);
        }

        /// <summary>
        ///     Registra índice
        /// </summary>
        /// <param name="key"></param>
        /// <param name="index"></param>
        public void RegisterIndex(Type key, string index)
        {
            var defaultIndices = Client.ConnectionSettings.DefaultIndices;
            if (!defaultIndices.ContainsKey(key))
                defaultIndices.Add(key, index);
        }

        /// <summary>
        ///     Registra índices
        /// </summary>
        /// <param name="indexes"></param>
        public void RegisterIndexes(IDictionary<Type, string> indexes)
        {
            foreach (var index in indexes)
                RegisterIndex(index.Key, index.Value);
        }

        /// <summary>
        ///     Realiza a criação de uma conexão
        /// </summary>
        /// <param name="debugProcess"></param>
        /// <param name="connection"></param>
        /// <param name="defaultIndex"></param>
        /// <returns></returns>
        public ElasticClient CreateConnection(Action<IApiCallDetails> debugProcess = null,
            IConnection connection = null, string defaultIndex = null)
        {
            if (_client != null) return _client;

            var pool = _endpoints.Count > 1
                ? (IConnectionPool)new SniffingConnectionPool(_endpoints)
                : new SingleNodeConnectionPool(_endpoints.First());
            var configs = new ConnectionSettings(pool, connection);

            if (_defaultIndex != null)
                configs.DefaultIndex(_defaultIndex);

            if (debugProcess != null)
                configs.EnableDebugMode(debugProcess);

            _client = new ElasticClient(configs);
            _lowLevelClient = new ElasticLowLevelClient(configs);

            return _client;
        }

        /// <summary>
        ///     Cria conexão caso Client for nulo
        /// </summary>
        public ElasticClient Client
        {
            get
            {
                if (_client == null)
                    CreateConnection();

                return _client;
            }
        }

        /// <summary>
        ///     Cria conexão caso LowLevelClient for nulo
        /// </summary>
        public ElasticLowLevelClient LowLevelClient
        {
            get
            {
                if (_lowLevelClient == null)
                    CreateConnection();

                return _lowLevelClient;
            }
        }
    }
}
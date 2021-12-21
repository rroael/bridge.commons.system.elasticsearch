using System;
using System.Collections.Generic;
using Elasticsearch.Net;
using Nest;

namespace Bridge.Commons.System.Elasticsearch.Contracts
{
    /// <summary>
    ///     CLiente Elastic Search
    /// </summary>
    public interface IElasticsearchClient
    {
        /// <summary>
        ///     Retorna o objeto de Client do Elasticsearch
        /// </summary>
        ElasticClient Client { get; }

        /// <summary>
        ///     Retorna o objeto de Client do Elasticsearch para requisições Low Level
        /// </summary>
        ElasticLowLevelClient LowLevelClient { get; }

        /// <summary>
        ///     Inicializa uma conexão com o elasticsearch, permite a configuração de um índice padrão, de uma conexão específica,
        ///     múltiplos mapeamentos do índice e um método para o Debug do serviço.
        /// </summary>
        /// <param name="indexes">Dicionário de índices mapeados por tipo de documento</param>
        /// <param name="debugProcess">Ação definida para processamento de debug do serviço</param>
        /// <param name="connection">Objeto de conexão do Elasticsearch</param>
        /// <param name="defaultIndex">Nome de um índice que será referênciado por padrão</param>
        void Init(IDictionary<Type, string> indexes = null, Action<IApiCallDetails> debugProcess = null,
            IConnection connection = null, string defaultIndex = null);

        /// <summary>
        ///     Retorna o client do elasticsearch
        /// </summary>
        /// <param name="debugProcess">Ação definida para processamento de debug do serviço</param>
        /// <param name="connection">Objeto de conexão do Elasticsearch</param>
        /// <param name="defaultIndex">Nome do índice padrão</param>
        /// <returns></returns>
        ElasticClient CreateConnection(Action<IApiCallDetails> debugProcess = null, IConnection connection = null,
            string defaultIndex = null);

        /// <summary>
        ///     Registra o índice por tipo nos mapeamentos automáticos do NEST
        /// </summary>
        /// <param name="key"></param>
        /// <param name="index"></param>
        void RegisterIndex(Type key, string index);

        /// <summary>
        ///     Registra uma lista de índices por tipo nos mapeamentos automáticos do NEST
        /// </summary>
        /// <param name="indexes"></param>
        void RegisterIndexes(IDictionary<Type, string> indexes);
    }
}
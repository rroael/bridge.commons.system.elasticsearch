using Bridge.Commons.System.Contracts.Mappers;

namespace Bridge.Commons.System.Elasticsearch.Contracts
{
    /// <summary>
    ///     Elastic search Input Output
    /// </summary>
    /// <typeparam name="TInput"></typeparam>
    /// <typeparam name="TOutput"></typeparam>
    public interface IElasticsearchDocument<TInput, out TOutput> : IObjectMapper<TInput, TOutput>
        where TInput : class
        where TOutput : class
    {
        /// <summary>
        ///     Buscar nome do indice
        /// </summary>
        /// <returns></returns>
        string GetIndexName();
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Bridge.Commons.System.Contracts;
using Bridge.Commons.System.Elasticsearch.Contracts;
using Bridge.Commons.System.Models.Results;
using Elasticsearch.Net;
using Nest;

namespace Bridge.Commons.System.Elasticsearch
{
    /// <summary>
    ///     Elastic Search base de repositório
    /// </summary>
    /// <typeparam name="TModel"></typeparam>
    /// <typeparam name="TDocument"></typeparam>
    public abstract class ElasticsearchBaseRepository<TModel, TDocument>
        where TModel : class
        where TDocument : class, IElasticsearchDocument<TModel, TDocument>, new()
    {
        private readonly IElasticsearchClient _client;

        #region Constructors

        /// <summary>
        ///     Construtor
        /// </summary>
        /// <param name="client"></param>
        public ElasticsearchBaseRepository(IElasticsearchClient client)
        {
            _client = client;
        }

        #endregion

        #region Documents

        /// <summary>
        ///     Documentação
        /// </summary>
        /// <param name="id"></param>
        /// <param name="indexName"></param>
        /// <returns></returns>
        protected async Task<ExistsResponse> DocumentExistsAsync(Id id, string indexName = null)
        {
            var doc = new DocumentPath<TDocument>(id);
            doc.Index(indexName ?? new TDocument().GetIndexName());
            return await Client.DocumentExistsAsync(doc);
        }

        #endregion


        #region Properties

        /// <summary>
        ///     Client
        /// </summary>
        public ElasticClient Client => _client.Client;

        /// <summary>
        ///     Client de baixo nível
        /// </summary>
        public ElasticLowLevelClient LowLevelClient => _client.LowLevelClient;

        #endregion

        #region Templates

        /// <summary>
        ///     Verifica se o template existe
        /// </summary>
        /// <param name="templateName">Nome do template</param>
        /// <returns>Booleano indicando se o template existe ou não</returns>
        protected async Task<bool> TemplateExistsAsync(string templateName)
        {
            try
            {
                var request = new IndexTemplateExistsRequest(Names.Parse(templateName));
                var result = await Client.Indices.TemplateExistsAsync(request);

                return result.Exists;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        ///     Cria um template se não existir
        /// </summary>
        /// <param name="templateName">Nome do template</param>
        /// <param name="numberOfReplicas">Número de réplicas do template</param>
        /// <param name="numberOfShards">Número de shards</param>
        /// <returns>Booleano indicando se o template foi criado ou não</returns>
        protected async Task<bool> CreateTemplateIfNotExistsAsync(string templateName, int numberOfReplicas = 1,
            int numberOfShards = 1)
        {
            var exists = await TemplateExistsAsync(templateName);

            if (exists)
                return false;

            var result = await Client.Indices.PutTemplateAsync(new Name(templateName),
                c => c
                    .Settings(s => s.NumberOfReplicas(numberOfReplicas).NumberOfShards(numberOfShards))
                    .IndexPatterns(templateName)
                    .Create()
                    .Order(0)
                    .Map<TDocument>(m => m.AutoMap()));
            //.Mappings(m => m.Map<TDocument>(mp => mp.AutoMap())));

            return result.IsValid;
        }

        #endregion

        #region Index

        /// <summary>
        ///     Verifica se o índice existe
        /// </summary>
        /// <param name="indexName">Nome do índice verificado</param>
        /// <returns>Booleano indicando se o índice existe ou não</returns>
        protected async Task<bool> IndexExistsAsync(string indexName)
        {
            var indexExistsRequest = new IndexExistsRequest(indexName);
            var indexExistsResult = await Client.Indices.ExistsAsync(indexExistsRequest);

            return indexExistsResult.Exists;
        }

        /// <summary>
        ///     Cria o índice se ele não existir
        /// </summary>
        /// <param name="indexName">Nome do índice verificado</param>
        /// <param name="mapping">Mapeamento do documento no índice, se for nulo o AutoMap será chamado</param>
        /// <param name="numberOfReplicas">Número de réplicas do template</param>
        /// <param name="numberOfShards">Número de shards</param>
        protected async Task CreateIndexIfNotExistsAsync(string indexName,
            Func<TypeMappingDescriptor<TDocument>, ITypeMapping> mapping = null, int numberOfReplicas = 1,
            int numberOfShards = 1)
        {
            var settings = new IndexSettings { NumberOfReplicas = numberOfReplicas, NumberOfShards = numberOfShards };
            var indexConfig = new IndexState { Settings = settings };

            var exists = await IndexExistsAsync(indexName);

            if (mapping == null)
                mapping = mp => mp.AutoMap();

            if (!exists)
                await Client.Indices.CreateAsync(indexName,
                    c => c
                        .InitializeUsing(indexConfig)
                        .Map(mapping));
            //.Mappings(m => m.Map<TDocument>(mapping)));
        }

        /// <summary>
        ///     Remove um índice
        /// </summary>
        /// <param name="indexName">Nome do índice a ser excluído</param>
        /// <param name="checkIfExists">Indica se o método deve verificar se o index existe antes de tentar excluir.</param>
        /// <returns>Objeto padrão de retorno de remover índice do NEST</returns>
        protected async Task<DeleteIndexResponse> DeleteIndexAsync(string indexName, bool checkIfExists = true)
        {
            if (checkIfExists && await IndexExistsAsync(indexName))
                return await Client.Indices.DeleteAsync(indexName);

            return await Task.FromResult(default(DeleteIndexResponse));
        }

        #endregion

        #region Select documents

        /// <summary>
        ///     Recupera um documento por Id
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo buscado</typeparam>
        /// <param name="id">Identificador do documento</param>
        /// <param name="indexName">Índice onde o documento será pesquisado, nulo se for por inferencia</param>
        /// <returns>Objeto padrão de retorno de get do NEST</returns>
        protected async Task<IGetResponse<TDocument>> GetAsync(Id id, string indexName = null)
        {
            var doc = new DocumentPath<TDocument>(id);
            doc.Index(indexName ?? new TDocument().GetIndexName());

            return await Client.GetAsync(doc);
        }

        /// <summary>
        ///     Recupera um documento e converte
        /// </summary>
        /// <param name="id">Identificador do documento</param>
        /// <param name="indexName">Índice onde o documento será pesquisado, nulo se for por inferencia</param>
        /// <returns>Objeto convertido para o modelo</returns>
        protected async Task<TModel> GetMappedAsync(Id id, string indexName = null)
        {
            var result = await GetAsync(id, indexName);

            return result?.Source?.MapTo();
        }

        /// <summary>
        ///     Pesquisa documentos baseado em um descritor
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo buscado</typeparam>
        /// <param name="query">Função do descritor de busca dos documentos</param>
        /// <returns>Objeto padrão de retorno de busca do NEST</returns>
        protected async Task<ISearchResponse<TDocument>> SearchAsync(
            Func<SearchDescriptor<TDocument>, ISearchRequest> query)
        {
            return await Client.SearchAsync(query);
        }

        /// <summary>
        ///     Pesquisa documentos baseado em um descritor e retorna uma lista de um modelo definido
        /// </summary>
        /// <param name="query">Descritor da busca</param>
        /// <typeparam name="TModel">Tipo do objeto do domínio a ser convertido</typeparam>
        /// <returns>Lista de objetos convertidos em um modelo definido</returns>
        protected async Task<IList<TModel>> SearchListAsync(Func<SearchDescriptor<TDocument>, ISearchRequest> query)
        {
            var searchResults = await Client.SearchAsync(query);

            return searchResults.Hits.Select(x => x.Source?.MapTo()).ToList();
        }

        /// <summary>
        ///     Pesquisa documentos baseado em um descritor e retorna uma lista paginada de um modelo definido
        /// </summary>
        /// <param name="query">Descritor da busca</param>
        /// <param name="pagination">Objeto de paginação</param>
        /// <typeparam name="TModel">Tipo do objeto do domínio a ser convertido</typeparam>
        /// <typeparam name="TPagination">Tipo do objeto de paginação</typeparam>
        /// <returns>Lista paginada de objetos convertidos em um modelo definido</returns>
        protected async Task<PaginatedList<TModel>> SearchPagedAsync<TPagination>(SearchDescriptor<TDocument> query,
            TPagination pagination)
            where TPagination : IPagination
        {
            var searchResults =
                await Client.SearchAsync<TDocument>(query.From(pagination.Skip).Take(pagination.PageSize));

            var totalCount = (int)searchResults.Total;
            var pageCount = (int)Math.Ceiling((double)totalCount / pagination.PageSize);

            return new PaginatedList<TModel>(searchResults.Hits.Select(x => x.Source.MapTo()), pagination.Page,
                pagination.PageSize, pageCount, totalCount);
        }

        /// <summary>
        ///     Pesquisa documentos baseado em um descritor e retorna todas as ocorrências, forçando o limite de retorno do
        ///     Elasticsearch
        /// </summary>
        /// <param name="query">Função do descritor de busca dos documentos</param>
        /// <returns>Objeto padrão de retorno de busca do NEST</returns>
        protected async Task<ISearchResponse<TDocument>> SearchAllAsync(
            Func<QueryContainerDescriptor<TDocument>, QueryContainer> query)
        {
            var searchResults = await Client.CountAsync<TDocument>(s => s.Query(query));

            return await Client.SearchAsync<TDocument>(s => s.Query(query).Size((int)searchResults.Count));
        }

        /// <summary>
        ///     Pesquisa documentos baseado em um descritor e retorna todas as ocorrências de um modelo definido, forçando o limite
        ///     de retorno do Elasticsearch
        /// </summary>
        /// <param name="query">Descritor da busca</param>
        /// <typeparam name="TModel">Tipo do objeto do domínio a ser convertido</typeparam>
        /// <returns>Lista de objetos convertidos em um modelo definido</returns>
        protected async Task<IList<TModel>> SearchAllMappedAsync(
            Func<QueryContainerDescriptor<TDocument>, QueryContainer> query)
        {
            var countResults = await Client.CountAsync<TDocument>(s => s.Query(query));
            var searchResults = await Client.SearchAsync<TDocument>(s => s.Query(query).Size((int)countResults.Count));

            return searchResults.Hits.Select(h => h.Source?.MapTo()).ToList();
        }

        #endregion

        #region Single document operations

        /// <summary>
        ///     Insere um documento
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo inserido</typeparam>
        /// <param name="doc">Documento que será armazenado</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> BulkInsertAsync(TDocument doc, string indexName = null)
        {
            return await BulkInsertAsync(new[] { doc }, indexName);
        }

        /// <summary>
        ///     Atualiza um documento
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo atualizado</typeparam>
        /// <param name="doc">Documento que será armazenado</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> BulkUpdateAsync(TDocument doc, string indexName = null)
        {
            return await BulkUpdateAsync(new[] { doc }, indexName);
        }

        /// <summary>
        ///     Remove um documento
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo atualizado</typeparam>
        /// <param name="doc">Documento que será armazenado</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> BulkDeleteAsync(TDocument doc, string indexName = null)
        {
            return await BulkDeleteAsync(new[] { doc }, indexName);
        }

        /// <summary>
        ///     Atualiza um documento forçando a atualização em caso de conflitos
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo atualizado</typeparam>
        /// <param name="doc">Documento que será armazenado</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <param name="retries">Número de tentativas que deverão ser efetuadas para salvar o documento</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> BulkUpsertAsync(TDocument doc, string indexName = null, int retries = 3)
        {
            return await BulkUpsertAsync(new[] { doc }, indexName, retries);
        }

        /// <summary>
        ///     Atualiza parcialmente um documento (propriedades desejadas)
        /// </summary>
        /// <typeparam name="TPartialDocument">Tipo do objeto que representa as propriedades que serão atualizadas</typeparam>
        /// <param name="partialUpdate">Documento com os valores que serão atualizados</param>
        /// <param name="docId">Identificador do documento do documento</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> PartialUpdateAsync<TPartialDocument>(TPartialDocument partialUpdate,
            Id docId, string indexName = null) where TPartialDocument : class
        {
            if (partialUpdate == null)
                return await Task.FromResult(default(BulkResponse));

            var descriptor = new BulkDescriptor();
            if (indexName != null)
                descriptor.Index(indexName);

            descriptor.Update<TDocument, TPartialDocument>(a => a.Id(docId).Doc(partialUpdate));

            return await Client.BulkAsync(descriptor);
        }

        /// <summary>
        ///     Atualiza um documento através de um script
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo atualizado</typeparam>
        /// <param name="id">Id do documento que será atualizado</param>
        /// <param name="script">Script que será executado</param>
        /// <param name="refreshShards">
        ///     If `true` then refresh the effected shards to make this operation visible to search, if
        ///     `wait_for` then wait for a refresh to make this operation visible to search, if `false` (the default) then do
        ///     nothing with refreshes.
        /// </param>
        /// <see cref="http://www.elastic.co/guide/en/elasticsearch/reference/7.6/docs-refresh.html" />
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <param name="parameters">Parâmetros que serão passado para a execução do script</param>
        /// <param name="language">Linguagem do script implementado</param>
        /// <param name="retryOnConflict">Quantidade de tentativas que deverão ser efetuadas na execução do script</param>
        /// <returns>Referencia de resposta Update padrão do NEST</returns>
        protected async Task<IUpdateResponse<TDocument>> ScriptUpdateAsync(string id, string script,
            Refresh refreshShards = Refresh.False, string indexName = null,
            Dictionary<string, object> parameters = null, string language = "painless", int retryOnConflict = 5)
        {
            var doc = new DocumentPath<TDocument>(id);
            doc.Index(indexName ?? new TDocument().GetIndexName());
            return await Client.UpdateAsync(doc,
                u => u.Script(
                        s => s
                            .Source(script)
                            .Lang(language)
                            .Params(parameters ?? new Dictionary<string, object>())
                    ).RetryOnConflict(retryOnConflict)
                    .Refresh(refreshShards));
        }

        /// <summary>
        ///     Atualiza um documento através de um script com uma query
        /// </summary>
        /// <param name="querySelector">Query a ser aplicada</param>
        /// <param name="script">Script que será executado</param>
        /// <param name="refreshShards">
        ///     If `true` then refresh the effected shards to make this operation visible to search, if
        ///     `false` (the default) then do nothing with refreshes.
        /// </param>
        /// <see cref="http://www.elastic.co/guide/en/elasticsearch/reference/7.6/docs-refresh.html" />
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <param name="parameters">Parâmetros que serão passado para a execução do script</param>
        /// <param name="language">Linguagem do script implementado</param>
        /// <param name="onConflicts">Proceed ou Abort</param>
        /// <returns></returns>
        protected async Task<UpdateByQueryResponse> ScriptUpdateByQueryAsync(
            Func<QueryContainerDescriptor<TDocument>, QueryContainer> querySelector, string script,
            bool refreshShards = false, string indexName = null, Dictionary<string, object> parameters = null,
            string language = "painless", Conflicts onConflicts = Conflicts.Proceed)
        {
            var descriptor = new UpdateByQueryDescriptor<TDocument>(indexName ?? new TDocument().GetIndexName());

            descriptor.Query(querySelector)
                .Script(s => s
                    .Source(script)
                    .Lang(language)
                    .Params(parameters ?? new Dictionary<string, object>())
                ).Conflicts(onConflicts)
                .Refresh(refreshShards);

            return await Client.UpdateByQueryAsync<TDocument>(d => descriptor);
        }

        #endregion

        #region Multiple document operations

        /// <summary>
        ///     Insere os documentos
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo atualizado</typeparam>
        /// <param name="documents">Lista de documento que serão armazenado</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> BulkInsertAsync(IEnumerable<TDocument> documents, string indexName = null)
        {
            var elasticsearchDocuments = documents as TDocument[] ?? documents.ToArray();
            if (elasticsearchDocuments.Count() < 1)
                return await Task.FromResult(default(BulkResponse));

            var bulkDescriptor = new BulkDescriptor();

            bulkDescriptor.Index(indexName ?? elasticsearchDocuments.First().GetIndexName());

            foreach (var doc in elasticsearchDocuments)
                bulkDescriptor.Index<TDocument>(op => op.Document(doc));

            return await Client.BulkAsync(bulkDescriptor);
        }

        /// <summary>
        ///     Atualiza os documentos
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo atualizado</typeparam>
        /// <param name="documents">Lista de documento que serão armazenado</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> BulkUpdateAsync(IEnumerable<TDocument> documents, string indexName = null)
        {
            var elasticsearchDocuments = documents as TDocument[] ?? documents.ToArray();
            if (elasticsearchDocuments.Count() < 1)
                return await Task.FromResult(default(BulkResponse));

            var bulkDescriptor = new BulkDescriptor();

            bulkDescriptor.Index(indexName ?? elasticsearchDocuments.First().GetIndexName());
            bulkDescriptor.Refresh(Refresh.True);

            foreach (var doc in elasticsearchDocuments)
                bulkDescriptor.Update<TDocument>(u => u.Doc(doc));


            return await Client.BulkAsync(bulkDescriptor);
        }

        /// <summary>
        ///     Remove os documentos
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo atualizado</typeparam>
        /// <param name="documents">Lista de documento que serão armazenado</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> BulkDeleteAsync(IEnumerable<TDocument> documents, string indexName = null)
        {
            var elasticsearchDocuments = documents as TDocument[] ?? documents.ToArray();
            if (elasticsearchDocuments.Count() < 1)
                return await Task.FromResult(default(BulkResponse));

            var bulkDescriptor = new BulkDescriptor();

            bulkDescriptor.Index(indexName ?? elasticsearchDocuments.First().GetIndexName());

            foreach (var doc in elasticsearchDocuments)
                bulkDescriptor.Delete<TDocument>(op => op.Document(doc));

            return await Client.BulkAsync(bulkDescriptor);
        }

        /// <summary>
        ///     Atualiza os documentos forçando a atualização em caso de conflitos
        /// </summary>
        /// <typeparam name="TDocument">Tipo do documento sendo atualizado</typeparam>
        /// <param name="documents">Lista de documento que serão armazenado</param>
        /// <param name="indexName">Índice onde o documento será inserido, nulo se for por inferencia</param>
        /// <param name="retries">Número de tentativas que deverão ser efetuadas para salvar o documento</param>
        /// <returns>Referencia de resposta Bulk padrão do NEST</returns>
        protected async Task<BulkResponse> BulkUpsertAsync(IEnumerable<TDocument> documents, string indexName = null,
            int retries = 3)
        {
            var elasticsearchDocuments = documents as TDocument[] ?? documents.ToArray();
            if (elasticsearchDocuments.Count() < 1)
                return await Task.FromResult(default(BulkResponse));

            var bulkDescriptor = new BulkDescriptor();

            bulkDescriptor.Index(indexName ?? elasticsearchDocuments.First().GetIndexName());
            bulkDescriptor.Refresh(Refresh.True);

            foreach (var doc in elasticsearchDocuments)
                bulkDescriptor.Update<TDocument>(u => u.Doc(doc).DocAsUpsert().RetriesOnConflict(retries));

            return await Client.BulkAsync(bulkDescriptor);
        }

        #endregion
    }
}
const util = require("util");
const { generateSaveQuery } = require("./generateQuery");

function Amazo() {
  this.dynamodb = {};
}

Amazo.prototype.model = function(schema) {
  const model = {};
  model.getDatabaseTableName = () =>
    schema.tableName ? schema.tableName : null;
  model.getHashKey = () => (schema.hashKey ? schema.hashKey : null);
  model.getSortKey = () => (schema.sortKey ? schema.sortKey : null);
  model.getIndex = () => (schema.index ? schema.index : null);

  model.findOne = (query = {}, options = { filters: {} }) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "findOne"),
      this.dynamoDocumentClient
    );
  };

  model.find = (query = {}, options = { filters: {} }) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "find"),
      this.dynamoDocumentClient
    );
  };

  model.scan = (query = {}, options = { filters: {} }) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "scan"),
      this.dynamoDocumentClient
    );
  };

  model.save = async (data = {}, options = {}) => {
    try {
      let dynamoQuery = generateSaveQuery(model, data, options);

      let result = await this.dynamodb.put(dynamoQuery);
      return result;
    } catch (error) {
      throw new error.QueryError(error.message);
    }
  };

  model.findOneAndUpdate = (
    query = {},
    data = {},
    options = { filters: {} }
  ) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "findOneAndUpdate"),
      this.dynamoDocumentClient,
      data
    );
  };

  model.findOneAndDelete = (query = {}, options = { filters: {} }) => {
    return new Query(
      model,
      query,
      options,
      (modelQuery = "findOneAndDelete"),
      this.dynamoDocumentClient
    );
  };

  return model;
};

/**
 * Document client for executing nested scans
 */

Amazo.prototype.setDocumentClient = function(documentClient) {
  this.dynamoDocumentClient = documentClient;
  for (prop in documentClient) {
    if (typeof documentClient[prop] === "function") {
      this.dynamodb[prop] = util.promisify(
        documentClient[prop].bind(documentClient)
      );
    }
  }
};

Amazo.prototype.Amazo = Amazo;

module.exports = new Amazo();
